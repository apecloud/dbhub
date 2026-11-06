/**
 * Runtime source synchronization with a protected control-plane API.
 *
 * ApeMind owns connection config; DBHub keeps only in-memory copies plus pools.
 * - PUT/DELETE /internal/sources/:id apply a single versioned source change.
 * - POST /internal/sync applies a full snapshot (startup fetch, periodic
 *   reconcile, control-plane push) and converges to it.
 * - All apply operations are fail-closed: a changed/disabled/deleted source is
 *   disconnected first (pool drained) and is never rolled back to the previous
 *   config when the new connect fails.
 * - Versions: a write carrying an older version than the applied one is
 *   rejected; equal versions are idempotent no-ops.
 */

import { Router, type Request, type Response } from "express";
import type { ConnectorManager } from "../connectors/manager.js";
import type { SourceConfig, TomlConfig, ToolConfig } from "../types/config.js";
import { initializeToolRegistry } from "../tools/registry.js";

export interface RuntimeSourceEntry {
  id: string;
  version: number;
  enabled: boolean;
  /** Source fields when enabled; dsn or discrete fields. */
  source?: Omit<SourceConfig, "id">;
  /** Tool options for the default builtin pair. */
  readonly?: boolean;
  max_rows?: number;
}

export interface RuntimeSnapshot {
  global_version: number;
  sources: RuntimeSourceEntry[];
}

interface AppliedEntry {
  version: number;
  enabled: boolean;
}

/** Serialize async apply operations so concurrent writers can't interleave. */
class Mutex {
  private tail: Promise<unknown> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.tail.then(fn, fn);
    this.tail = next.catch(() => undefined);
    return next;
  }
}

export class RuntimeSyncStore {
  private entries = new Map<string, AppliedEntry>();
  private _globalVersion = 0;

  get globalVersion(): number {
    return this._globalVersion;
  }

  versionOf(id: string): number {
    return this.entries.get(id)?.version ?? -1;
  }

  isEnabled(id: string): boolean {
    return this.entries.get(id)?.enabled === true;
  }

  mark(id: string, version: number, enabled: boolean): void {
    this.entries.set(id, { version, enabled });
  }

  unmark(id: string): void {
    this.entries.delete(id);
  }

  setGlobalVersion(v: number): void {
    this._globalVersion = v;
  }

  ids(): string[] {
    return [...this.entries.keys()];
  }
}

function toolsForEntry(entry: RuntimeSourceEntry): ToolConfig[] {
  return [
    {
      name: "execute_sql",
      source: entry.id,
      readonly: entry.readonly !== false,
      ...(entry.max_rows !== undefined ? { max_rows: entry.max_rows } : {}),
    },
    { name: "search_objects", source: entry.id },
  ];
}

function refreshToolRegistry(live: Map<string, RuntimeSourceEntry>): void {
  const sources: SourceConfig[] = [];
  const tools: ToolConfig[] = [];
  for (const [id, e] of live) {
    if (!e.enabled || !e.source) continue;
    sources.push({ id, ...e.source });
    tools.push(...toolsForEntry(e));
  }
  const config: TomlConfig = { sources, tools };
  initializeToolRegistry(config);
}

/**
 * Apply one versioned source change, fail-closed.
 * Returns 'applied' | 'noop' or throws with status for the caller to map.
 */
async function applyEntry(
  manager: ConnectorManager,
  store: RuntimeSyncStore,
  live: Map<string, RuntimeSourceEntry>,
  entry: RuntimeSourceEntry
): Promise<"applied" | "noop"> {
  const current = store.versionOf(entry.id);
  if (entry.version === current) {
    return "noop";
  }
  if (entry.version < current) {
    const err = new Error(`stale version ${entry.version} < ${current}`);
    (err as Error & { status?: number }).status = 409;
    throw err;
  }

  if (!entry.enabled || !entry.source) {
    // disable/delete: fail-closed — drain pool first, tombstone stays.
    await manager.removeSource(entry.id);
    live.delete(entry.id);
    store.mark(entry.id, entry.version, false);
    refreshToolRegistry(live);
    return "applied";
  }

  const sourceConfig: SourceConfig = { id: entry.id, ...entry.source };

  // Close the old pool before attempting the new config: never serve a
  // tightened/rotated source from the previous connection (no rollback).
  await manager.removeSource(entry.id);
  try {
    if (sourceConfig.lazy) {
      await manager.addSource(sourceConfig);
    } else {
      await manager.addSource(sourceConfig);
    }
  } catch (error) {
    // New config failed: source stays absent (fail-closed), version recorded
    // so retries with the same or newer version can still apply.
    live.delete(entry.id);
    store.mark(entry.id, entry.version, false);
    refreshToolRegistry(live);
    const err = new Error(
      `source '${entry.id}' failed to connect; left disabled (fail-closed): ${(error as Error).message}`
    );
    (err as Error & { status?: number }).status = 502;
    throw err;
  }
  live.set(entry.id, entry);
  store.mark(entry.id, entry.version, true);
  refreshToolRegistry(live);
  return "applied";
}

/** Apply a full snapshot; converges local state to it. */
async function applySnapshot(
  manager: ConnectorManager,
  store: RuntimeSyncStore,
  live: Map<string, RuntimeSourceEntry>,
  snapshot: RuntimeSnapshot
): Promise<{ applied: string[]; errors: string[] }> {
  if (snapshot.global_version < store.globalVersion) {
    const err = new Error(
      `stale snapshot ${snapshot.global_version} < ${store.globalVersion}`
    );
    (err as Error & { status?: number }).status = 409;
    throw err;
  }
  const applied: string[] = [];
  const errors: string[] = [];
  const wanted = new Map(snapshot.sources.map((s) => [s.id, s]));

  for (const entry of snapshot.sources) {
    try {
      if (await applyEntry(manager, store, live, entry) === "applied") {
        applied.push(entry.id);
      }
    } catch (error) {
      errors.push(`${entry.id}: ${(error as Error).message}`);
    }
  }

  // Delete local sources absent from the snapshot (full reconcile).
  for (const id of store.ids()) {
    if (!wanted.has(id)) {
      await manager.removeSource(id);
      live.delete(id);
      store.unmark(id);
      applied.push(`-${id}`);
    }
  }
  refreshToolRegistry(live);
  store.setGlobalVersion(snapshot.global_version);
  return { applied, errors };
}

export function createInternalRouter(
  manager: ConnectorManager,
  store: RuntimeSyncStore,
  serviceToken: string
): Router {
  const router = Router();
  const mutex = new Mutex();
  const live = new Map<string, RuntimeSourceEntry>();
  // Expose for the sync loop to share the same serialized apply path.
  (router as unknown as { _ctx: object })._ctx = { live, mutex };

  router.use((req: Request, res: Response, next) => {
    if (req.headers.authorization !== `Bearer ${serviceToken}`) {
      return res.status(401).json({ error: "unauthorized" });
    }
    next();
  });

  router.get("/sources", (_req: Request, res: Response) => {
    res.json({
      global_version: store.globalVersion,
      sources: store.ids().map((id) => ({
        id,
        version: store.versionOf(id),
        enabled: store.isEnabled(id),
        connected: (manager as unknown as { connectors?: Map<string, unknown> }).connectors?.has(id) ?? undefined,
      })),
    });
  });

  router.put("/sources/:id", async (req: Request, res: Response) => {
    const body = req.body as Partial<RuntimeSourceEntry>;
    const entry: RuntimeSourceEntry = {
      id: req.params.id,
      version: Number(body.version),
      enabled: body.enabled !== false,
      source: body.source,
      readonly: body.readonly,
      max_rows: body.max_rows,
    };
    if (!Number.isInteger(entry.version) || entry.version < 0) {
      return res.status(400).json({ error: "version must be a non-negative integer" });
    }
    try {
      const result = await mutex.run(() => applyEntry(manager, store, live, entry));
      res.json({ ok: true, result, version: entry.version });
    } catch (error) {
      const e = error as Error & { status?: number };
      res.status(e.status ?? 500).json({ error: e.message });
    }
  });

  router.delete("/sources/:id", async (req: Request, res: Response) => {
    const version = Number(req.query.version ?? store.versionOf(req.params.id) + 1);
    const entry: RuntimeSourceEntry = { id: req.params.id, version, enabled: false };
    try {
      const result = await mutex.run(() => applyEntry(manager, store, live, entry));
      res.json({ ok: true, result, version });
    } catch (error) {
      const e = error as Error & { status?: number };
      res.status(e.status ?? 500).json({ error: e.message });
    }
  });

  router.post("/sync", async (req: Request, res: Response) => {
    const snapshot = req.body as RuntimeSnapshot;
    if (!snapshot || !Array.isArray(snapshot.sources) || !Number.isInteger(snapshot.global_version)) {
      return res.status(400).json({ error: "expected {global_version, sources[]}" });
    }
    try {
      const outcome = await mutex.run(() => applySnapshot(manager, store, live, snapshot));
      res.json({ ok: outcome.errors.length === 0, ...outcome });
    } catch (error) {
      const e = error as Error & { status?: number };
      res.status(e.status ?? 500).json({ error: e.message });
    }
  });

  return router;
}

export interface SyncLoopOptions {
  controlPlaneUrl: string;
  serviceToken: string;
  replicaId: string;
  syncUrl: string;
  reconcileIntervalMs?: number;
  fetchImpl?: typeof fetch;
}

/**
 * Startup fetch + replica registration + long-poll + periodic reconcile.
 * Each replica recovers independently; the push path is best-effort only.
 */
export function startSyncLoop(
  router: Router,
  manager: ConnectorManager,
  store: RuntimeSyncStore,
  opts: SyncLoopOptions
): () => void {
  const { live, mutex } = (router as unknown as { _ctx: { live: Map<string, RuntimeSourceEntry>; mutex: Mutex } })._ctx;
  const fetchImpl = opts.fetchImpl ?? fetch;
  const reconcileMs = opts.reconcileIntervalMs ?? 30_000;
  let stopped = false;
  let timer: NodeJS.Timeout | null = null;
  let abort = new AbortController();

  const headers = { Authorization: `Bearer ${opts.serviceToken}`, "Content-Type": "application/json" };
  const base = opts.controlPlaneUrl.replace(/\/$/, "");

  const pull = async (sinceVersion?: number): Promise<RuntimeSnapshot | null> => {
    const url = sinceVersion === undefined
      ? `${base}/internal/dbhub/sources`
      : `${base}/internal/dbhub/sources?since_version=${sinceVersion}`;
    const resp = await fetchImpl(url, { headers, signal: abort.signal });
    if (!resp.ok) throw new Error(`snapshot fetch failed: ${resp.status}`);
    return (await resp.json()) as RuntimeSnapshot;
  };

  const applyRemote = (snap: RuntimeSnapshot) =>
    mutex.run(() => applySnapshot(manager, store, live, snap));

  const register = async () => {
    try {
      await fetchImpl(`${base}/internal/dbhub/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({ replica_id: opts.replicaId, sync_url: opts.syncUrl }),
      });
    } catch (error) {
      console.error(`runtime-sync: replica register failed (retrying): ${(error as Error).message}`);
    }
  };

  const longPoll = async () => {
    while (!stopped) {
      try {
        const snap = await pull(store.globalVersion);
        if (stopped || !snap) return;
        if (snap.global_version > store.globalVersion) {
          await applyRemote(snap);
        }
      } catch (error) {
        if (stopped) return;
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
  };

  const reconcile = async () => {
    if (stopped) return;
    try {
      const snap = await pull();
      if (snap) await applyRemote(snap);
    } catch (error) {
      console.error(`runtime-sync: reconcile failed: ${(error as Error).message}`);
    } finally {
      if (!stopped) timer = setTimeout(reconcile, reconcileMs);
    }
  };

  void (async () => {
    await register();
    try {
      const snap = await pull();
      if (snap) await applyRemote(snap);
    } catch (error) {
      console.error(`runtime-sync: initial snapshot failed (reconcile will retry): ${(error as Error).message}`);
    }
    void longPoll();
    timer = setTimeout(reconcile, reconcileMs);
  })();

  return () => {
    stopped = true;
    if (timer) clearTimeout(timer);
    abort.abort();
    abort = new AbortController();
  };
}

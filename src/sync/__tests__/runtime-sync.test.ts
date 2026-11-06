import { describe, it, expect, beforeEach, vi } from "vitest";
import express from "express";
import type { AddressInfo } from "net";

import { createInternalRouter, RuntimeSyncStore, type RuntimeSourceEntry } from "../runtime-sync.js";

vi.mock("../../tools/registry.js", () => ({
  initializeToolRegistry: vi.fn(),
}));

class FakeManager {
  sources = new Map<string, unknown>();
  added: string[] = [];
  removed: string[] = [];
  failOn = new Set<string>();

  async addSource(s: { id: string }): Promise<void> {
    if (this.failOn.has(s.id)) throw new Error("connect failed");
    this.sources.set(s.id, s);
    this.added.push(s.id);
  }
  async removeSource(id: string): Promise<void> {
    this.sources.delete(id);
    this.removed.push(id);
  }
}

async function serve(router: express.Router): Promise<{ base: string; close: () => void }> {
  const app = express();
  app.use(express.json());
  app.use("/internal", router);
  const server = await new Promise<import("http").Server>((resolve) => {
    const s = app.listen(0, "127.0.0.1", () => resolve(s));
  });
  const port = (server.address() as AddressInfo).port;
  return { base: `http://127.0.0.1:${port}/internal`, close: () => server.close() };
}

const TOKEN = "svc-token";
const headers = { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" };

const src = (id: string, version: number, extra: Partial<RuntimeSourceEntry> = {}): RuntimeSourceEntry => ({
  id,
  version,
  enabled: true,
  source: { type: "postgres", host: "pg", database: "db", user: "u", password: "p" },
  ...extra,
});

describe("runtime sync internal API", () => {
  let manager: FakeManager;
  let store: RuntimeSyncStore;
  let base: string;
  let close: () => void;

  beforeEach(async () => {
    manager = new FakeManager();
    store = new RuntimeSyncStore();
    const router = createInternalRouter(manager as never, store, TOKEN);
    ({ base, close } = await serve(router));
  });

  it("rejects requests without the service token", async () => {
    const res = await fetch(`${base}/sources`);
    expect(res.status).toBe(401);
    close();
  });

  it("adds a source and lists it", async () => {
    const put = await fetch(`${base}/sources/fin_pg`, {
      method: "PUT",
      headers,
      body: JSON.stringify(src("fin_pg", 1)),
    });
    expect(put.status).toBe(200);
    expect(manager.sources.has("fin_pg")).toBe(true);

    const list = (await (await fetch(`${base}/sources`, { headers })).json()) as { sources: { id: string }[] };
    expect(list.sources[0]).toMatchObject({ id: "fin_pg", version: 1, enabled: true });
    close();
  });

  it("rejects an older version, accepts same-version idempotent retry", async () => {
    await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 3)) });

    const older = await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 2)) });
    expect(older.status).toBe(409);

    const same = await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 3)) });
    expect(same.status).toBe(200);
    const body = (await same.json()) as { result: string };
    expect(body.result).toBe("noop");
    close();
  });

  it("fail-closed: failed new config leaves source absent, no rollback", async () => {
    await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 1)) });
    manager.failOn.add("fin_pg");

    const res = await fetch(`${base}/sources/fin_pg`, {
      method: "PUT",
      headers,
      body: JSON.stringify(src("fin_pg", 2)),
    });
    expect(res.status).toBe(502);
    expect(manager.sources.has("fin_pg")).toBe(false);
    // The removal ran before the (failed) re-add: old pool is gone.
    expect(manager.removed).toContain("fin_pg");
    close();
  });

  it("disable drains the pool and tombstones the version", async () => {
    await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 1)) });
    const res = await fetch(`${base}/sources/fin_pg`, {
      method: "PUT",
      headers,
      body: JSON.stringify({ version: 2, enabled: false }),
    });
    expect(res.status).toBe(200);
    expect(manager.sources.has("fin_pg")).toBe(false);
    expect(store.isEnabled("fin_pg")).toBe(false);
    expect(store.versionOf("fin_pg")).toBe(2);
    close();
  });

  it("snapshot converges: adds, updates, removes absent ids", async () => {
    await fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", 1)) });
    await fetch(`${base}/sources/old_db`, { method: "PUT", headers, body: JSON.stringify(src("old_db", 1)) });

    const res = await fetch(`${base}/sync`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        global_version: 5,
        sources: [src("fin_pg", 2), src("fin_mysql", 1, { source: { type: "mysql", host: "m", database: "d", user: "u", password: "p" } })],
      }),
    });
    expect(res.status).toBe(200);
    expect(manager.sources.has("fin_mysql")).toBe(true);
    expect(manager.sources.has("old_db")).toBe(false);
    expect(store.globalVersion).toBe(5);
    close();
  });

  it("rejects out-of-order snapshots", async () => {
    await fetch(`${base}/sync`, { method: "POST", headers, body: JSON.stringify({ global_version: 5, sources: [] }) });
    const res = await fetch(`${base}/sync`, { method: "POST", headers, body: JSON.stringify({ global_version: 3, sources: [] }) });
    expect(res.status).toBe(409);
    close();
  });

  it("serializes concurrent puts without corrupting state", async () => {
    const results = await Promise.all(
      [1, 2, 3].map((v) =>
        fetch(`${base}/sources/fin_pg`, { method: "PUT", headers, body: JSON.stringify(src("fin_pg", v)) }).then((r) => r.status)
      )
    );
    // Exactly the applied writes succeed; stale ones are 409 — final state is version 3.
    expect(results.filter((s) => s === 200).length).toBeGreaterThanOrEqual(1);
    expect(store.versionOf("fin_pg")).toBe(3);
    close();
  });
});

import { test } from "node:test";
import assert from "node:assert/strict";
import { WorkerPool } from "../src/pool.ts";
import { fingerprint, makeRows, transformChunk, type ExportedRow } from "../src/task.ts";
import type { ChunkRequest } from "../src/worker.ts";

const WORKER_URL = new URL("../src/worker.ts", import.meta.url);

function makePool(size = 2): WorkerPool<ChunkRequest, ExportedRow[]> {
  return new WorkerPool<ChunkRequest, ExportedRow[]>(WORKER_URL, size);
}

test("pooled output is identical to running the transform inline", async () => {
  const rows = makeRows(2_000);
  const pool = makePool(3);
  try {
    const chunks = [rows.slice(0, 700), rows.slice(700, 1_400), rows.slice(1_400)];
    const results = await Promise.all(chunks.map((chunk) => pool.run({ rows: chunk, work: 4 })));
    assert.equal(fingerprint(results.flat()), fingerprint(transformChunk(rows, 4)));
  } finally {
    await pool.destroy();
  }
});

test("more tasks than workers queue instead of spawning threads", async () => {
  const pool = makePool(2);
  try {
    const rows = makeRows(100);
    const tasks = Array.from({ length: 12 }, () => pool.run({ rows, work: 2 }));
    assert.ok(pool.pending > 0, "expected tasks to be waiting for a free worker");
    const results = await Promise.all(tasks);
    assert.equal(results.length, 12);
    assert.equal(pool.pending, 0);
    for (const result of results) assert.equal(result.length, 100);
    assert.equal(pool.size, 2);
  } finally {
    await pool.destroy();
  }
});

test("results stay matched to their own task", async () => {
  const pool = makePool(2);
  try {
    const sizes = [10, 250, 4, 90, 1_000, 7];
    const results = await Promise.all(
      sizes.map((size) => pool.run({ rows: makeRows(size), work: 2 })),
    );
    assert.deepEqual(results.map((result) => result.length), sizes);
  } finally {
    await pool.destroy();
  }
});

test("a throwing task rejects and leaves the pool usable", async () => {
  const pool = makePool(2);
  try {
    await assert.rejects(
      pool.run({ rows: makeRows(10), work: 1, fail: true }),
      /deliberate worker failure/,
    );
    const after = await pool.run({ rows: makeRows(10), work: 1 });
    assert.equal(after.length, 10);
    assert.equal(pool.size, 2);
  } finally {
    await pool.destroy();
  }
});

test("destroy rejects queued work and refuses new work", async () => {
  const pool = makePool(1);
  const inFlight = pool.run({ rows: makeRows(5_000), work: 6 });
  const queued = pool.run({ rows: makeRows(10), work: 1 });

  // Attach the handlers before destroying: destroy() settles these
  // synchronously, and a rejection nobody is listening for yet is an
  // unhandled rejection, not a test failure.
  const queuedRejects = assert.rejects(queued, /destroyed/);
  const inFlightSettled = inFlight.catch(() => undefined);

  await pool.destroy();

  await queuedRejects;
  await inFlightSettled;
  await assert.rejects(pool.run({ rows: makeRows(1), work: 1 }), /destroyed/);
});

test("pool size must be at least one", () => {
  assert.throws(() => new WorkerPool(WORKER_URL, 0), RangeError);
});

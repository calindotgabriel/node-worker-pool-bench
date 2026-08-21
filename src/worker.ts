import { parentPort } from "node:worker_threads";
import { makeRows, transformChunk, type ExportedRow, type Row } from "./task.ts";

/**
 * Two ways to hand a worker its slice of the job:
 *
 * - `rows` — the caller structured-clones the payload across the thread
 *   boundary. Simple, and the version most examples show.
 * - `range` — the caller sends two numbers and the worker materialises its own
 *   rows. Only viable when the data is derivable or already reachable from the
 *   worker (a file, a cursor, a query), which in an export pipeline it usually
 *   is.
 *
 * The benchmark runs both, because the difference between them is larger than
 * the difference either one makes against the main thread.
 */
export interface ChunkRequest {
  work: number;
  rows?: Row[];
  range?: { start: number; count: number };
  /** Test hook: makes the worker throw, so error propagation is exercised. */
  fail?: boolean;
}

if (!parentPort) {
  throw new Error("worker.ts must be started as a worker thread");
}

const port = parentPort;

port.on("message", (request: ChunkRequest) => {
  try {
    if (request.fail) throw new Error("deliberate worker failure");
    const rows = request.rows ?? makeRows(request.range?.count ?? 0, request.range?.start ?? 0);
    const result: ExportedRow[] = transformChunk(rows, request.work);
    port.postMessage({ ok: true, result });
  } catch (error) {
    const err = error as Error;
    port.postMessage({ ok: false, error: { message: err.message, stack: err.stack } });
  }
});

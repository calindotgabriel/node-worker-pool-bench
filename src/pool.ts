import { Worker } from "node:worker_threads";
import { once } from "node:events";

/**
 * A fixed-size worker pool.
 *
 * Deliberately small. It exists to make one thing measurable — what happens to
 * the main thread when CPU-bound work is moved off it — not to compete with
 * Piscina. The parts that matter for that measurement are the ones people get
 * wrong when they hand-roll this: tasks queue instead of spawning threads,
 * every task settles exactly once, a worker that throws rejects its own task
 * and stays in the pool, and a worker that dies is replaced rather than
 * silently leaving the pool one thread short.
 */

interface PendingTask<TPayload, TResult> {
  payload: TPayload;
  resolve: (value: TResult) => void;
  reject: (reason: Error) => void;
}

interface Slot {
  worker: Worker;
  busy: boolean;
  /** Settles the task this worker is currently running, if any. */
  settle: ((error: Error | null, result: unknown) => void) | null;
}

export interface WorkerMessage {
  ok: boolean;
  result?: unknown;
  error?: { message: string; stack?: string };
}

export class WorkerPool<TPayload = unknown, TResult = unknown> {
  readonly size: number;
  readonly #workerUrl: URL;
  readonly #slots: Slot[] = [];
  readonly #queue: PendingTask<TPayload, TResult>[] = [];
  #destroyed = false;

  constructor(workerUrl: URL, size: number) {
    if (size < 1) throw new RangeError(`pool size must be >= 1, got ${size}`);
    this.#workerUrl = workerUrl;
    this.size = size;
    for (let i = 0; i < size; i += 1) this.#spawn();
  }

  get pending(): number {
    return this.#queue.length;
  }

  #spawn(): Slot {
    const worker = new Worker(this.#workerUrl);
    const slot: Slot = { worker, busy: false, settle: null };

    worker.on("message", (message: WorkerMessage) => {
      const settle = slot.settle;
      slot.settle = null;
      slot.busy = false;
      if (settle) {
        if (message.ok) {
          settle(null, message.result);
        } else {
          const error = new Error(message.error?.message ?? "worker task failed");
          if (message.error?.stack) error.stack = message.error.stack;
          settle(error, undefined);
        }
      }
      this.#drain();
    });

    // A worker that dies mid-task must reject that task, not hang it, and the
    // pool must not quietly shrink.
    worker.on("error", (error: Error) => this.#replace(slot, error));
    worker.on("exit", (code) => {
      if (this.#destroyed || code === 0) return;
      this.#replace(slot, new Error(`worker exited with code ${code}`));
    });

    this.#slots.push(slot);
    return slot;
  }

  #replace(slot: Slot, error: Error): void {
    const settle = slot.settle;
    slot.settle = null;
    slot.busy = false;
    settle?.(error, undefined);

    const index = this.#slots.indexOf(slot);
    if (index !== -1) this.#slots.splice(index, 1);
    void slot.worker.terminate();

    if (!this.#destroyed) {
      this.#spawn();
      this.#drain();
    }
  }

  #drain(): void {
    while (this.#queue.length > 0) {
      const slot = this.#slots.find((candidate) => !candidate.busy);
      if (!slot) return;
      const task = this.#queue.shift()!;
      slot.busy = true;
      slot.settle = (error, result) => {
        if (error) task.reject(error);
        else task.resolve(result as TResult);
      };
      slot.worker.postMessage(task.payload);
    }
  }

  run(payload: TPayload): Promise<TResult> {
    if (this.#destroyed) {
      return Promise.reject(new Error("pool has been destroyed"));
    }
    return new Promise<TResult>((resolve, reject) => {
      this.#queue.push({ payload, resolve, reject });
      this.#drain();
    });
  }

  async destroy(): Promise<void> {
    this.#destroyed = true;

    const drained = this.#queue.splice(0, this.#queue.length);
    for (const task of drained) task.reject(new Error("pool has been destroyed"));

    // Terminating a busy worker means its task will never report back. Settle
    // it here, or the caller waits on a promise that can no longer resolve.
    for (const slot of this.#slots) {
      const settle = slot.settle;
      slot.settle = null;
      slot.busy = false;
      settle?.(new Error("pool has been destroyed while the task was running"), undefined);
    }

    await Promise.all(
      this.#slots.map(async (slot) => {
        const exit = once(slot.worker, "exit");
        await slot.worker.terminate();
        await exit;
      }),
    );
    this.#slots.length = 0;
  }
}

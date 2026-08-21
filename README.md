# node-worker-pool-bench

Moving CPU-bound work into a worker pool is standard advice. This measures what it actually buys
you, and shows the version of it that makes things worse.

I did this for real once: an export on an energy-market platform ran for 45 minutes and pinned the
main thread while it did. Moving it into worker pools brought it to about 12. This repository is the
technique from that job, isolated, instrumented and reproducible on your own machine — because a
number in a CV is worth roughly nothing next to a command you can run.

```bash
npm install
npm run bench
```

Node 22.7+ (TypeScript runs directly, no build step). Two dev dependencies, both for typechecking.

## What is measured

Two questions, because only asking the first one is how people end up disappointed:

1. **Throughput** — wall-clock time for the whole export.
2. **Responsiveness** — could the process still have answered anything while the export ran? A 10 ms
   interval stands in for a health check; the table reports how many of those ticks arrived within
   25 ms of when they were due. `node:perf_hooks`' `monitorEventLoopDelay` gives the precise view
   alongside it.

The second one is usually the real reason to reach for workers. An export that is 30% faster and
takes the API down with it is not an improvement.

Three scenarios, all producing byte-identical output — the benchmark fingerprints every result set
and refuses to report if they disagree, because a faster wrong answer is not a result:

| Scenario | What it does |
| --- | --- |
| `inline` | Transform on the main thread, yielding between chunks |
| `pool, rows copied` | Post the rows to the pool; results come back the same way |
| `pool, range sent` | Post two numbers; the worker materialises its own slice |

## Results

MacBook, Node v24.7.0, 200,000 rows, 9 workers, median of 3 runs.

**Chunks of 50,000 rows** — the shape most exports start out as:

| scenario | wall (ms) | loop p99 (ms) | loop max (ms) | health checks on time |
| --- | ---: | ---: | ---: | ---: |
| inline (main thread) | 557 | 138.4 | 138.4 | **0.0%** |
| pool, rows copied (9w) | 272 | 1.4 | 1.5 | 55.6% |
| pool, range sent (9w) | 250 | 1.7 | 57.3 | **65.4%** |

The main thread answers **nothing** on time while the inline export runs. That is the finding. The
2.2x on wall-clock is the part people quote; the 0% is the part that pages someone at 3am.

**Chunks of 5,000 rows** — after someone has already added `await` between chunks:

| scenario | wall (ms) | loop p99 (ms) | loop max (ms) | health checks on time |
| --- | ---: | ---: | ---: | ---: |
| inline (main thread) | 555 | 18.1 | 18.1 | 69.1% |
| pool, rows copied (9w) | 159 | 35.7 | 35.7 | **43.8%** |
| pool, range sent (9w) | 133 | 23.7 | 23.7 | 71.4% |

Read the middle row. **Copying rows into the pool made the process less responsive than doing the
work inline** — 43.8% against 69.1% — while still being 3.5x faster overall. Structured clone is not
free and it is not concurrent: serialising every chunk out and every result back happens on the main
thread, the one the pool was supposed to free. Enough small chunks and you have simply exchanged
compute on the main thread for serialisation on the main thread.

Sending a range instead — two numbers, worker builds its own slice — is faster *and* keeps the
process answering. In an export pipeline this is usually available: the worker can hold its own
cursor, read its own file offset, run its own query.

Your numbers will differ. Run it.

```bash
npm run bench -- --rows 500000 --chunk 50000 --workers 4 --work 60
npm run bench -- --json > results/my-machine.json
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rows` | 200000 | Rows in the export |
| `--chunk` | 5000 | Rows per task |
| `--workers` | cores − 1 | Pool size |
| `--work` | 60 | CPU cost per row |
| `--runs` | 3 | Repetitions; the table reports medians |
| `--json` | off | Machine-readable output |

## What is in here

| File | |
| --- | --- |
| `src/pool.ts` | The pool. Fixed size, queue, one settlement per task, workers replaced when they die |
| `src/worker.ts` | Worker entry — accepts either a copied payload or a range |
| `src/task.ts` | The CPU-bound transform under test. Pure, deterministic, no I/O |
| `src/lag.ts` | Responsiveness probe — loop-delay histogram plus the heartbeat |
| `src/bench.ts` | Runs the scenarios, verifies they agree, prints the table |

The pool is deliberately small — about 130 lines. It is not competing with
[Piscina](https://github.com/piscinajs/piscina), which you should use in production. It exists so
the thing being measured is visible in one file, and the parts it does take seriously are the ones
that are easy to get wrong by hand:

- Tasks **queue**. More tasks than workers must not mean more threads.
- Every task settles **exactly once**, including when its worker dies mid-task.
- A worker that throws rejects **its own** task and stays in the pool.
- A worker that exits unexpectedly is **replaced**, so the pool does not quietly shrink.
- `destroy()` settles in-flight work instead of leaving those promises pending forever. The test
  suite found that one: the first version terminated busy workers and left their callers waiting on
  a promise that could no longer settle. See `destroy rejects queued work and refuses new work`.

```bash
npm test        # 10 tests, node:test, no framework
npm run typecheck
```

## Caveats

- Synthetic CPU work. A transform that allocates heavily, or touches I/O, will behave differently —
  I/O-bound work does not belong in a worker pool at all.
- Single machine, single run of results. The point is the shape of the difference, not the constants.
- Thread startup is excluded: workers are warmed before the measured window. Cold pools cost more,
  which is an argument for keeping a pool rather than spawning per job.
- `--work` sets CPU cost per row. Turn it down far enough and the transfer cost dominates, which is
  the second table's finding pushed to its extreme.

## Licence

MIT.

import { availableParallelism } from "node:os";
import { mkdirSync, writeFileSync } from "node:fs";
import { WorkerPool } from "./pool.ts";
import { startProbe, tick, type Responsiveness } from "./lag.ts";
import { fingerprint, makeRows, transformChunk, type ExportedRow, type Row } from "./task.ts";
import type { ChunkRequest } from "./worker.ts";

interface Options {
  rows: number;
  runs: number;
  workers: number;
  chunk: number;
  work: number;
  json: boolean;
}

interface RunResult {
  wallMs: number;
  responsiveness: Responsiveness;
  fingerprint: string;
}

interface ScenarioResult {
  name: string;
  runs: RunResult[];
  medianWallMs: number;
  medianP99Ms: number;
  medianOnTimeRatio: number;
  fingerprint: string;
}

function parseArgs(argv: string[]): Options {
  const defaults: Options = {
    rows: 200_000,
    runs: 3,
    workers: Math.max(2, availableParallelism() - 1),
    chunk: 5_000,
    work: 60,
    json: false,
  };
  const options = { ...defaults };
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (flag === "--json") {
      options.json = true;
      continue;
    }
    const value = argv[i + 1];
    if (!flag?.startsWith("--") || value === undefined) continue;
    const key = flag.slice(2) as keyof Options;
    if (key in defaults && key !== "json") {
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`invalid value for ${flag}: ${value}`);
      }
      (options[key] as number) = Math.floor(parsed);
      i += 1;
    }
  }
  return options;
}

function chunkRows(rows: Row[], size: number): Row[][] {
  const chunks: Row[][] = [];
  for (let i = 0; i < rows.length; i += size) chunks.push(rows.slice(i, i + size));
  return chunks;
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid]!;
  return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

/**
 * Everything on the main thread — the shape most export endpoints start out as.
 * The `await tick()` between chunks is a concession in the blocking run's
 * favour: without it the loop would never get a look-in at all.
 */
async function runInline(chunks: Row[][], work: number): Promise<RunResult> {
  const probe = startProbe();
  const started = process.hrtime.bigint();
  const out: ExportedRow[] = [];
  for (const chunk of chunks) {
    out.push(...transformChunk(chunk, work));
    await tick();
  }
  const wallMs = Number(process.hrtime.bigint() - started) / 1e6;
  return { wallMs, responsiveness: probe.stop(), fingerprint: fingerprint(out) };
}

/**
 * The same work, dispatched to a fixed pool of threads.
 *
 * `transfer: "copy"` posts the rows themselves, so every chunk is
 * structured-cloned out and the results cloned back — and both halves of that
 * cost land on the main thread, which is the thread the pool was supposed to
 * free. `transfer: "range"` posts two numbers instead.
 */
async function runPooled(
  chunks: Row[][],
  work: number,
  workers: number,
  transfer: "copy" | "range",
): Promise<RunResult> {
  const pool = new WorkerPool<ChunkRequest, ExportedRow[]>(
    new URL("./worker.ts", import.meta.url),
    workers,
  );
  try {
    // Warm the threads so thread startup is not charged to the measured window.
    await Promise.all(
      Array.from({ length: workers }, () => pool.run({ rows: [], work: 1 })),
    );

    let start = 0;
    const requests: ChunkRequest[] = chunks.map((rows) => {
      const request: ChunkRequest =
        transfer === "copy"
          ? { rows, work }
          : { range: { start, count: rows.length }, work };
      start += rows.length;
      return request;
    });

    const probe = startProbe();
    const started = process.hrtime.bigint();
    const results = await Promise.all(requests.map((request) => pool.run(request)));
    const wallMs = Number(process.hrtime.bigint() - started) / 1e6;
    return { wallMs, responsiveness: probe.stop(), fingerprint: fingerprint(results.flat()) };
  } finally {
    await pool.destroy();
  }
}

function summarise(name: string, runs: RunResult[]): ScenarioResult {
  return {
    name,
    runs,
    medianWallMs: median(runs.map((run) => run.wallMs)),
    medianP99Ms: median(runs.map((run) => run.responsiveness.loopDelayP99Ms)),
    medianOnTimeRatio: median(runs.map((run) => run.responsiveness.heartbeatOnTimeRatio)),
    fingerprint: runs[0]!.fingerprint,
  };
}

function pad(value: string, width: number): string {
  return value.length >= width ? value : `${value}${" ".repeat(width - value.length)}`;
}

function padStart(value: string, width: number): string {
  return value.length >= width ? value : `${" ".repeat(width - value.length)}${value}`;
}

function report(options: Options, scenarios: ScenarioResult[]): void {
  const header = ["scenario", "wall (ms)", "loop p99 (ms)", "loop max (ms)", "health checks on time"];
  const rows = scenarios.map((scenario) => {
    const maxMs = median(scenario.runs.map((run) => run.responsiveness.loopDelayMaxMs));
    return [
      scenario.name,
      scenario.medianWallMs.toFixed(0),
      scenario.medianP99Ms.toFixed(1),
      maxMs.toFixed(1),
      `${(scenario.medianOnTimeRatio * 100).toFixed(1)}%`,
    ];
  });

  const widths = header.map((cell, column) =>
    Math.max(cell.length, ...rows.map((row) => row[column]!.length)),
  );

  console.log("");
  console.log(
    `rows=${options.rows}  chunk=${options.chunk}  work=${options.work}  workers=${options.workers}  runs=${options.runs}  node=${process.version}`,
  );
  console.log("");
  console.log(header.map((cell, i) => (i === 0 ? pad(cell, widths[i]!) : padStart(cell, widths[i]!))).join("  "));
  console.log(widths.map((width) => "-".repeat(width)).join("  "));
  for (const row of rows) {
    console.log(row.map((cell, i) => (i === 0 ? pad(cell, widths[i]!) : padStart(cell, widths[i]!))).join("  "));
  }

  const [inline, ...pooled] = scenarios;
  if (inline && pooled.length > 0) {
    console.log("");
    for (const scenario of pooled) {
      const speedup = inline.medianWallMs / scenario.medianWallMs;
      console.log(
        `${pad(scenario.name, 28)} ${speedup.toFixed(2)}x wall-clock  ·  health checks on time ${(inline.medianOnTimeRatio * 100).toFixed(1)}% -> ${(scenario.medianOnTimeRatio * 100).toFixed(1)}%`,
      );
    }
  }
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const rows = makeRows(options.rows);
  const chunks = chunkRows(rows, options.chunk);

  const inlineRuns: RunResult[] = [];
  const copyRuns: RunResult[] = [];
  const rangeRuns: RunResult[] = [];
  for (let run = 0; run < options.runs; run += 1) {
    inlineRuns.push(await runInline(chunks, options.work));
    copyRuns.push(await runPooled(chunks, options.work, options.workers, "copy"));
    rangeRuns.push(await runPooled(chunks, options.work, options.workers, "range"));
  }

  const scenarios = [
    summarise("inline (main thread)", inlineRuns),
    summarise(`pool, rows copied (${options.workers}w)`, copyRuns),
    summarise(`pool, range sent (${options.workers}w)`, rangeRuns),
  ];

  // A faster wrong answer is not a result. Both paths must agree exactly.
  const [first, ...rest] = scenarios;
  for (const scenario of rest) {
    if (scenario.fingerprint !== first!.fingerprint) {
      throw new Error(
        `scenarios disagree: ${first!.name}=${first!.fingerprint} ${scenario.name}=${scenario.fingerprint}`,
      );
    }
  }

  if (options.json) {
    console.log(JSON.stringify({ options, scenarios }, null, 2));
  } else {
    report(options, scenarios);
    console.log(`output verified identical across scenarios (${first!.fingerprint})`);
  }

  mkdirSync(new URL("../results/", import.meta.url), { recursive: true });
  writeFileSync(
    new URL("../results/latest.json", import.meta.url),
    `${JSON.stringify({ node: process.version, platform: process.platform, options, scenarios }, null, 2)}\n`,
  );
}

await main();

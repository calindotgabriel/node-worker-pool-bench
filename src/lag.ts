import { monitorEventLoopDelay } from "node:perf_hooks";

/**
 * Measures whether the process could still have answered anything while the
 * export was running.
 *
 * Two views of the same question. `monitorEventLoopDelay` is the precise one —
 * a native histogram of how late the loop ran. The heartbeat is the legible
 * one: a 10 ms interval standing in for a health check, counting how many ticks
 * arrived on time. A blocked main thread shows up as a handful of very late
 * ticks; a pooled run shows up as almost all of them on time.
 */

const HEARTBEAT_MS = 10;
const ON_TIME_BUDGET_MS = 25;

export interface Responsiveness {
  loopDelayMeanMs: number;
  loopDelayP99Ms: number;
  loopDelayMaxMs: number;
  heartbeatsExpected: number;
  heartbeatsOnTime: number;
  heartbeatOnTimeRatio: number;
  worstHeartbeatGapMs: number;
}

export interface ResponsivenessProbe {
  stop: () => Responsiveness;
}

export function startProbe(): ResponsivenessProbe {
  const histogram = monitorEventLoopDelay({ resolution: 1 });
  histogram.enable();

  const startedAt = process.hrtime.bigint();
  let last = startedAt;
  let onTime = 0;
  let ticks = 0;
  let worstGapMs = 0;

  const timer = setInterval(() => {
    const now = process.hrtime.bigint();
    const gapMs = Number(now - last) / 1e6;
    last = now;
    ticks += 1;
    if (gapMs > worstGapMs) worstGapMs = gapMs;
    if (gapMs <= ON_TIME_BUDGET_MS) onTime += 1;
  }, HEARTBEAT_MS);
  timer.unref();

  return {
    stop(): Responsiveness {
      clearInterval(timer);
      histogram.disable();
      const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
      const expected = Math.max(1, Math.round(elapsedMs / HEARTBEAT_MS));
      return {
        loopDelayMeanMs: histogram.mean / 1e6,
        loopDelayP99Ms: histogram.percentile(99) / 1e6,
        loopDelayMaxMs: histogram.max / 1e6,
        heartbeatsExpected: expected,
        heartbeatsOnTime: onTime,
        heartbeatOnTimeRatio: onTime / expected,
        worstHeartbeatGapMs: worstGapMs,
      };
    },
  };
}

/** Yields to the loop so a pending timer can actually fire. */
export function tick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

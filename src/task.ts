/**
 * The unit of work under test.
 *
 * It stands in for the row-level transform in a large export: read a record,
 * derive a few fields, and compute a checksum over the result. It is pure,
 * deterministic and CPU-bound on purpose — no I/O, no timers, no crypto. The
 * whole point of the benchmark is that this work cannot be made to yield, so
 * wherever it runs, it occupies that thread completely.
 */

export interface Row {
  id: number;
  account: string;
  region: string;
  kwh: number;
}

export interface ExportedRow {
  id: number;
  label: string;
  checksum: string;
}

const REGIONS = ["north", "south", "east", "west", "central"] as const;

/**
 * Deterministic synthetic input, so any two runs are comparable — and
 * addressable by range, so a worker can build its own slice from two numbers
 * instead of being sent the rows. That difference turns out to matter more
 * than the thread count; see the third scenario in the benchmark.
 */
export function makeRows(count: number, start = 0): Row[] {
  const rows: Row[] = new Array(count);
  for (let offset = 0; offset < count; offset += 1) {
    const i = start + offset;
    rows[offset] = {
      id: i,
      account: `ACC-${(i * 7919) % 1_000_000}`,
      region: REGIONS[i % REGIONS.length]!,
      kwh: ((i * 37) % 9_000) + 100,
    };
  }
  return rows;
}

/** FNV-1a, iterated `work` times to make the per-row cost tunable. */
function checksum(input: string, work: number): string {
  let hash = 0x811c9dc5;
  for (let pass = 0; pass < work; pass += 1) {
    for (let i = 0; i < input.length; i += 1) {
      hash ^= input.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    hash = (hash + pass) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

export function transformRow(row: Row, work: number): ExportedRow {
  const label = `${row.region.toUpperCase()}/${row.account}/${row.kwh}`;
  return { id: row.id, label, checksum: checksum(label, work) };
}

export function transformChunk(rows: Row[], work: number): ExportedRow[] {
  const out: ExportedRow[] = new Array(rows.length);
  for (let i = 0; i < rows.length; i += 1) {
    out[i] = transformRow(rows[i]!, work);
  }
  return out;
}

/** Collapses a result set to one value, so runs can be compared for equality. */
export function fingerprint(rows: ExportedRow[]): string {
  let hash = 0x811c9dc5;
  for (const row of rows) {
    for (let i = 0; i < row.checksum.length; i += 1) {
      hash ^= row.checksum.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    hash = (hash ^ row.id) >>> 0;
  }
  return `${rows.length}:${hash.toString(16).padStart(8, "0")}`;
}

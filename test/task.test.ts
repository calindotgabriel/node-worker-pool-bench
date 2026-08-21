import { test } from "node:test";
import assert from "node:assert/strict";
import { fingerprint, makeRows, transformChunk, transformRow } from "../src/task.ts";

test("input generation is deterministic", () => {
  assert.deepEqual(makeRows(50), makeRows(50));
});

test("the transform is pure and repeatable", () => {
  const row = makeRows(1)[0]!;
  assert.deepEqual(transformRow(row, 8), transformRow(row, 8));
});

test("more work per row changes the checksum, not the shape", () => {
  const row = makeRows(1)[0]!;
  const cheap = transformRow(row, 1);
  const expensive = transformRow(row, 32);
  assert.equal(cheap.id, expensive.id);
  assert.equal(cheap.label, expensive.label);
  assert.notEqual(cheap.checksum, expensive.checksum);
});

test("fingerprint separates different result sets", () => {
  const rows = makeRows(100);
  assert.equal(fingerprint(transformChunk(rows, 3)), fingerprint(transformChunk(rows, 3)));
  assert.notEqual(fingerprint(transformChunk(rows, 3)), fingerprint(transformChunk(rows, 4)));
  assert.notEqual(
    fingerprint(transformChunk(rows, 3)),
    fingerprint(transformChunk(rows.slice(0, 99), 3)),
  );
});

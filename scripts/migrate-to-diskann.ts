#!/usr/bin/env bun
/**
 * Migrate vectors_vec from brute-force vec0 to DiskANN-indexed vec0.
 *
 * Single-shot migration: CREATE new table, INSERT all rows, DROP old, RENAME.
 * Uses sqlite-vec@0.1.10-alpha.4 (must be installed in this checkout).
 *
 * Usage:
 *   bun scripts/migrate-to-diskann.ts --index-path /tmp/qmd-diskann/index.sqlite
 */
import { Database } from "bun:sqlite";
import * as sqliteVec from "sqlite-vec";
import { existsSync, statSync } from "node:fs";

const argv = process.argv.slice(2);
let indexPath = "";
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === "--index-path" && argv[i + 1]) {
    indexPath = argv[i + 1]!;
    i++;
  }
}
if (!indexPath) {
  console.error("usage: bun migrate-to-diskann.ts --index-path <path>");
  process.exit(1);
}
if (!existsSync(indexPath)) {
  console.error(`index not found: ${indexPath}`);
  process.exit(1);
}

const fileSizeMB = (statSync(indexPath).size / 1024 / 1024).toFixed(0);
console.log(`Migrating ${indexPath} (${fileSizeMB} MB)`);

Database.setCustomSQLite("/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib");
const db = new Database(indexPath);
sqliteVec.load(db);

// Disable WAL during migration. The DiskANN graph build does a large
// INSERT...SELECT that grew the WAL past 3 GB on a previous attempt and
// exhausted disk space. We're on a CLONE — if anything goes wrong here,
// re-clone from PROD. Restore WAL after migration completes.
const origJournal = (db.prepare("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode;
console.log(`Original journal_mode: ${origJournal} — switching to OFF for migration`);
db.exec("PRAGMA journal_mode = OFF");
const newJournal = (db.prepare("PRAGMA journal_mode").get() as { journal_mode: string }).journal_mode;
console.log(`Migration journal_mode: ${newJournal}`);

const vecVersion = (db.prepare("SELECT vec_version() AS v").get() as { v: string }).v;
console.log(`sqlite-vec version: ${vecVersion}`);
if (!vecVersion.includes("alpha")) {
  console.error("ERROR: expected alpha sqlite-vec for DiskANN support");
  process.exit(1);
}

// Confirm starting state
const rowCount = (db.prepare("SELECT COUNT(*) AS c FROM vectors_vec").get() as { c: number }).c;
console.log(`Current vectors_vec row count: ${rowCount}`);

// Confirm no leftover migration table from a failed prior run
const existsNew = db.prepare(
  "SELECT name FROM sqlite_master WHERE name='vectors_vec_diskann'"
).get();
if (existsNew) {
  console.error("ERROR: vectors_vec_diskann already exists — aborting (clean up first)");
  process.exit(1);
}

// Step 1: CREATE new table with DiskANN index
const t0 = Date.now();
console.log("\n[1/4] Creating vectors_vec_diskann (DiskANN, int8, n_neighbors=32, search_list_size=64)...");
db.exec(`
  CREATE VIRTUAL TABLE vectors_vec_diskann USING vec0(
    hash_seq TEXT PRIMARY KEY,
    embedding float[768] distance_metric=cosine
      INDEXED BY diskann(neighbor_quantizer=int8, n_neighbors=32, search_list_size=64)
  )
`);
console.log(`  done in ${Date.now() - t0}ms`);

// Step 2: INSERT all rows (this is the slow part — DiskANN must build the graph)
const t1 = Date.now();
console.log(`\n[2/4] Inserting ${rowCount} rows (this builds the Vamana graph; may take 5-15 min)...`);
const inserted = db.prepare(
  "INSERT INTO vectors_vec_diskann SELECT hash_seq, embedding FROM vectors_vec"
).run();
const insertMs = Date.now() - t1;
console.log(`  done in ${(insertMs / 1000).toFixed(1)}s (${insertMs} ms)`);

// Verify counts match
const newCount = (db.prepare("SELECT COUNT(*) AS c FROM vectors_vec_diskann").get() as { c: number }).c;
console.log(`  new row count: ${newCount} (expected ${rowCount})`);
if (newCount !== rowCount) {
  console.error("ERROR: row count mismatch — aborting before DROP");
  process.exit(1);
}

// Step 3: DROP old table
const t2 = Date.now();
console.log("\n[3/4] Dropping old vectors_vec...");
db.exec("DROP TABLE vectors_vec");
console.log(`  done in ${Date.now() - t2}ms`);

// Step 4: RENAME new table to vectors_vec
const t3 = Date.now();
console.log("\n[4/4] Renaming vectors_vec_diskann -> vectors_vec...");
db.exec("ALTER TABLE vectors_vec_diskann RENAME TO vectors_vec");
console.log(`  done in ${Date.now() - t3}ms`);

// Confirm final state
const finalCount = (db.prepare("SELECT COUNT(*) AS c FROM vectors_vec").get() as { c: number }).c;
const finalSchema = (db.prepare(
  "SELECT sql FROM sqlite_master WHERE name='vectors_vec'"
).get() as { sql: string }).sql;
console.log(`\nFinal vectors_vec row count: ${finalCount}`);
console.log(`Final schema:\n  ${finalSchema}`);

// Restore WAL mode before closing
console.log(`\nRestoring journal_mode = ${origJournal}`);
db.exec(`PRAGMA journal_mode = ${origJournal}`);

db.close();
const totalMs = Date.now() - t0;
console.log(`\nMigration complete in ${(totalMs / 1000).toFixed(1)}s`);

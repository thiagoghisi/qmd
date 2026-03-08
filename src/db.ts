/**
 * db.ts - Cross-runtime SQLite compatibility layer
 *
 * Provides a unified Database export that works under both Bun (bun:sqlite)
 * and Node.js (better-sqlite3). The APIs are nearly identical — the main
 * difference is the import path.
 */

import { debug } from "./debug.js";

export const isBun = typeof globalThis.Bun !== "undefined";

let _Database: any;
let _sqliteVecLoad: (db: any) => void;

if (isBun) {
  // Dynamic string prevents tsc from resolving bun:sqlite on Node.js builds
  const bunSqlite = "bun:" + "sqlite";
  _Database = (await import(/* @vite-ignore */ bunSqlite)).Database;
  const { getLoadablePath } = await import("sqlite-vec");
  _sqliteVecLoad = (db: any) => db.loadExtension(getLoadablePath());
} else {
  _Database = (await import("better-sqlite3")).default;
  const sqliteVec = await import("sqlite-vec");
  _sqliteVecLoad = (db: any) => sqliteVec.load(db);
}

/**
 * Open a SQLite database. Works with both bun:sqlite and better-sqlite3.
 */
export function openDatabase(path: string): Database {
  const t0 = Date.now();
  debug("db.open", "opening database", { path });
  const db = new _Database(path) as Database;
  debug("db.open", `done in ${Date.now() - t0}ms`);
  return db;
}

/**
 * Common subset of the Database interface used throughout QMD.
 */
export interface Database {
  exec(sql: string): void;
  prepare(sql: string): Statement;
  loadExtension(path: string): void;
  close(): void;
}

export interface Statement {
  run(...params: any[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: any[]): any;
  all(...params: any[]): any[];
}

/**
 * Load the sqlite-vec extension into a database.
 */
export function loadSqliteVec(db: Database): void {
  const t0 = Date.now();
  try {
    _sqliteVecLoad(db);
    debug("db.ext", `sqlite-vec loaded in ${Date.now() - t0}ms`);
  } catch (err) {
    debug("db.ext", `sqlite-vec load failed in ${Date.now() - t0}ms`, { error: String(err) });
    throw err;
  }
}

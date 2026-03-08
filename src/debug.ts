/**
 * QMD Debug Logger
 *
 * Writes structured debug logs to ~/.cache/qmd/debug.log
 * Enable with QMD_DEBUG=1 environment variable.
 *
 * Usage:
 *   import { debug } from "./debug.js";
 *   debug("search", "BM25 query", { query, resultCount: 42 });
 */

import { appendFileSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { resolve } from "node:path";

// Debug logging is ON by default. Set QMD_NO_DEBUG=1 to disable.
const enabled = !process.env.QMD_NO_DEBUG;
let logPath: string | undefined;

function getLogPath(): string {
  if (!logPath) {
    const cacheDir = process.env.XDG_CACHE_HOME
      ? resolve(process.env.XDG_CACHE_HOME, "qmd")
      : resolve(homedir(), ".cache", "qmd");
    mkdirSync(cacheDir, { recursive: true });
    logPath = resolve(cacheDir, "debug.log");
  }
  return logPath;
}

export function debug(component: string, message: string, data?: Record<string, unknown>): void {
  if (!enabled) return;
  try {
    const ts = new Date().toISOString();
    const dataStr = data ? " " + JSON.stringify(data) : "";
    appendFileSync(getLogPath(), `${ts} [${component}] ${message}${dataStr}\n`);
  } catch {
    // Never fail on logging
  }
}

export function isDebugEnabled(): boolean {
  return enabled;
}

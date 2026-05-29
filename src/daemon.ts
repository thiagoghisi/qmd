#!/usr/bin/env bun
/**
 * QMD Daemon - Persistent background process that keeps LLM models warm.
 *
 * Architecture:
 *   qmd daemon start   → spawns background process, preloads models, listens on Unix socket
 *   qmd daemon stop    → sends shutdown signal via socket
 *   qmd daemon status  → checks if daemon is running
 *   qmd daemon warmup  → sends a warmup query to preload all models
 *
 * CLI integration:
 *   qmd query/vsearch  → auto-detects daemon via socket, forwards if available
 *
 * Socket protocol: newline-delimited JSON over Unix domain socket.
 *   Request:  { "command": "query"|"vsearch"|"search"|"shutdown"|"status"|"warmup", "args": {...} }
 *   Response: { "ok": true, "data": ... } or { "ok": false, "error": "..." }
 *   Streaming: For search results, sends { "type": "stderr", "data": "..." } for progress,
 *              then final { "ok": true, "data": ... }
 */

import { unlinkSync, existsSync, readFileSync, writeFileSync, mkdirSync, statSync } from "node:fs";
import { debug } from "./debug.js";
import { createServer, connect, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import type { RankedResult } from "./store.js";

// =============================================================================
// Paths and constants
// =============================================================================

const RUNTIME_DIR = join(homedir(), ".cache", "qmd");
const SOCKET_PATH = join(RUNTIME_DIR, "daemon.sock");
const PID_FILE = join(RUNTIME_DIR, "daemon.pid");

export { SOCKET_PATH, PID_FILE, RUNTIME_DIR };

type DaemonRankedResult = RankedResult & {
  hash?: string;
  docid?: string;
};

// =============================================================================
// Daemon client (used by CLI to check/connect to daemon)
// =============================================================================

/**
 * Check if a daemon is running and reachable.
 */
export function isDaemonRunning(): boolean {
  if (!existsSync(PID_FILE)) {
    debug("daemon.isRunning", "no PID file", { selfPid: process.pid, selfPpid: process.ppid });
    return false;
  }

  try {
    const pid = parseInt(readFileSync(PID_FILE, "utf-8").trim(), 10);
    // Check if process is alive (signal 0 = just check)
    process.kill(pid, 0);
    // Process alive — check socket
    if (existsSync(SOCKET_PATH)) {
      debug("daemon.isRunning", "alive with socket", { targetPid: pid, selfPid: process.pid });
      return true;
    }

    // Process alive but socket missing. This is either a normal startup window
    // (daemon binding socket after writing PID) or a zombie (process stuck after
    // crash). Either way, the CLIENT is not the right place to kill or clean up
    // — that's launchd's job via KeepAlive=true. We just report not-ready;
    // launchd will reap unresponsive daemons.
    debug("daemon.isRunning", "alive but socket missing — not killing (let launchd handle)", {
      targetPid: pid,
      selfPid: process.pid,
    });
    return false;
  } catch {
    // Process not running. Leave the stale PID file alone — the next daemon
    // startup (via launchd KeepAlive) will overwrite it. Touching files from
    // the client is what caused the cascading destruction in the first place.
    debug("daemon.isRunning", "PID file points to dead process, returning false", {
      selfPid: process.pid,
    });
    return false;
  }
}

/**
 * Send a command to the daemon and return the response.
 */
export function sendCommand(command: string, args: Record<string, unknown> = {}): Promise<{
  ok: boolean;
  data?: unknown;
  error?: string;
  stderr?: string;
}> {
  return new Promise((resolve, reject) => {
    const socket = connect(SOCKET_PATH);
    let buffer = "";
    let stderrChunks: string[] = [];
    let timeout: ReturnType<typeof setTimeout>;

    // 3 minute timeout for queries (models may need to process)
    timeout = setTimeout(() => {
      socket.destroy();
      reject(new Error("Daemon request timed out (180s)"));
    }, 180_000);

    socket.on("connect", () => {
      socket.write(JSON.stringify({ command, args }) + "\n");
    });

    socket.on("data", (chunk) => {
      buffer += chunk.toString();

      // Process complete lines
      let newlineIdx: number;
      while ((newlineIdx = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newlineIdx);
        buffer = buffer.slice(newlineIdx + 1);

        try {
          const msg = JSON.parse(line);

          // Streaming stderr (progress messages)
          if (msg.type === "stderr") {
            stderrChunks.push(msg.data);
            // Write progress to stderr in real time
            process.stderr.write(msg.data);
            continue;
          }

          // Final response
          clearTimeout(timeout);
          socket.end();
          resolve({ ...msg, stderr: stderrChunks.join("") });
        } catch {
          // Ignore malformed lines
        }
      }
    });

    socket.on("error", (err: NodeJS.ErrnoException) => {
      clearTimeout(timeout);
      if (err.code === "ECONNREFUSED" || err.code === "ENOENT") {
        // Daemon might be restarting, busy, or genuinely down. Return error
        // without touching any files — client should not be deleting the
        // daemon's PID file just because one connection failed.
        debug("daemon.sendCommand", "socket error, returning error (no cleanup)", {
          command,
          code: err.code,
          selfPid: process.pid,
        });
        resolve({ ok: false, error: "Daemon not reachable" });
      } else {
        reject(err);
      }
    });

    socket.on("close", () => {
      clearTimeout(timeout);
      // If we didn't get a response yet, the daemon closed unexpectedly
      if (buffer.trim()) {
        try {
          resolve(JSON.parse(buffer));
        } catch {
          resolve({ ok: false, error: "Incomplete response from daemon" });
        }
      }
    });
  });
}

function cleanupStaleFiles(reason: string = "unspecified"): void {
  // Observability: capture WHO is deleting PID/socket and what state they're in.
  // This is investigating churn where client-side cleanup deletes a live daemon's
  // files, causing launchd to spawn a duplicate daemon.
  let sockInfo: Record<string, unknown> = { exists: false };
  let pidInfo: Record<string, unknown> = { exists: false };
  try {
    if (existsSync(SOCKET_PATH)) {
      const s = statSync(SOCKET_PATH);
      sockInfo = { exists: true, ageMs: Date.now() - s.mtimeMs };
    }
  } catch {}
  try {
    if (existsSync(PID_FILE)) {
      const s = statSync(PID_FILE);
      const content = readFileSync(PID_FILE, "utf-8").trim();
      pidInfo = { exists: true, ageMs: Date.now() - s.mtimeMs, pid: content };
    }
  } catch {}
  debug("daemon.cleanup", "cleanupStaleFiles called", {
    reason,
    selfPid: process.pid,
    selfPpid: process.ppid,
    sock: sockInfo,
    pid: pidInfo,
  });
  try { unlinkSync(SOCKET_PATH); } catch {}
  try { unlinkSync(PID_FILE); } catch {}
}

// =============================================================================
// Daemon server (runs in background process)
// =============================================================================

/**
 * Start the daemon server. This function never returns (runs until shutdown).
 */
export async function startDaemonServer(): Promise<void> {
  // Ensure runtime dir exists
  mkdirSync(RUNTIME_DIR, { recursive: true });

  debug("daemon.start", "startDaemonServer entered", {
    selfPid: process.pid,
    selfPpid: process.ppid,
    argv: process.argv.slice(2).join(" "),
  });

  // Write our PID FIRST so concurrent status checks see a live daemon during
  // the startup window (heavy async imports below take 3-30s). The bash
  // launchd script also writes the PID before exec; this just overwrites with
  // the same PID (exec preserves it). Doing the cleanup AFTER would erase the
  // PID file that bash just wrote and create a race window.
  writeFileSync(PID_FILE, String(process.pid));
  debug("daemon.start", "wrote PID file early", { pidFile: PID_FILE, pid: process.pid });

  // Remove only a stale socket file (we will bind a fresh one). Never touch
  // the PID file from here — touching it is what caused cascade restarts.
  try { unlinkSync(SOCKET_PATH); } catch {}

  // Late-import heavy modules only in the daemon process
  const { enableProductionMode, createStore, searchFTS, searchVec, extractSnippet, getContextForFile,
    reciprocalRankFusion, DEFAULT_EMBED_MODEL, DEFAULT_QUERY_MODEL, DEFAULT_RERANK_MODEL,
  } = await import("./store.js");
  const { getDefaultLlamaCpp, disposeDefaultLlamaCpp, withLLMSession } = await import("./llm.js");
  const { getCollection: getCollectionFromYaml } = await import("./collections.js");

  enableProductionMode();
  const store = createStore();

  /** Normalize collection arg: string, string[], or undefined → string[] */
  function normalizeCollection(raw: unknown): string[] {
    if (!raw) return [];
    if (Array.isArray(raw)) return raw;
    return [raw as string];
  }

  function collectionScopes(collection: string[]): (string | undefined)[] {
    return collection.length > 0 ? collection : [undefined];
  }

  function byScoreDesc<T extends { score: number }>(results: T[]): T[] {
    return results.sort((a, b) => b.score - a.score);
  }

  console.error(`[qmd daemon] PID ${process.pid}, socket ${SOCKET_PATH}`);
  console.error(`[qmd daemon] Database opened, ${store.getStatus().totalDocuments} documents indexed`);

  // -------------------------------------------------------------------------
  // Handle incoming commands
  // -------------------------------------------------------------------------

  async function handleCommand(
    command: string,
    args: Record<string, unknown>,
    sendStderr: (msg: string) => void
  ): Promise<{ ok: boolean; data?: unknown; error?: string }> {
    const cmdStart = Date.now();
    debug("daemon", `>>> ${command}`, args);
    try {
    const result = await (async (): Promise<{ ok: boolean; data?: unknown; error?: string }> => {
    switch (command) {
      case "status": {
        const status = store.getStatus();
        const llm = getDefaultLlamaCpp();
        return {
          ok: true,
          data: {
            pid: process.pid,
            uptime: Math.floor(process.uptime()),
            ...status,
            modelsLoaded: {
              embed: !!(llm as any).embedModel,
              generate: !!(llm as any).generateModel,
              rerank: !!(llm as any).rerankModel,
            },
          },
        };
      }

      case "warmup": {
        sendStderr("[qmd daemon] Warming up models...\n");
        const llm = getDefaultLlamaCpp();

        const start = Date.now();
        const loaded: string[] = [];
        const failed: string[] = [];
        let warmupEmbedding: number[] | null = null;

        await withLLMSession(async (session) => {
          // Load embed + generate in parallel
          const results = await Promise.allSettled([
            session.expandQuery("test warmup query").then(() => { loaded.push("generate"); }),
            session.embed("test warmup text").then((r) => {
              loaded.push("embed");
              warmupEmbedding = r?.embedding ?? null;
            }),
          ]);
          for (const r of results) {
            if (r.status === "rejected") {
              const name = loaded.length === 0 ? "generate" : "embed";
              failed.push(name);
              sendStderr(`[qmd daemon] Warning: ${name} model failed to load: ${r.reason}\n`);
            }
          }

          // Rerank needs docs — load separately so failures are clearly attributed
          try {
            await session.rerank("test", [{ file: "test.md", text: "warmup text" }]);
            loaded.push("rerank");
          } catch (e) {
            failed.push("rerank");
            sendStderr(`[qmd daemon] Warning: rerank model failed to load: ${e}\n`);
          }
        });

        // Page-cache prime for the sqlite-vec index.
        // Without this, the first real vsearch of the day pays a one-time
        // cold-I/O penalty (~17s on a 4.2 GB / 540K-vector index) as the OS
        // pulls index pages off disk. Running one tiny MATCH query here forces
        // those pages into the OS page cache so subsequent user queries serve
        // from RAM (sub-second per scan).
        if (warmupEmbedding) {
          try {
            const vecStart = Date.now();
            const tableExists = store.db.prepare(
              `SELECT name FROM sqlite_master WHERE type='table' AND name='vectors_vec'`
            ).get();
            if (tableExists) {
              store.db.prepare(
                `SELECT hash_seq FROM vectors_vec WHERE embedding MATCH ? AND k = 1`
              ).all(new Float32Array(warmupEmbedding));
              const vecMs = Date.now() - vecStart;
              sendStderr(`[qmd daemon] vec-index primed in ${(vecMs / 1000).toFixed(1)}s\n`);
              debug("daemon.warmup", "vec-index primed", { scanMs: vecMs });
              loaded.push("vec-index");
            }
          } catch (e) {
            failed.push("vec-index");
            sendStderr(`[qmd daemon] Warning: vec-index prime failed: ${e}\n`);
          }
        }

        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        if (failed.length > 0) {
          sendStderr(`[qmd daemon] Warmup partial: ${loaded.join(", ")} loaded; ${failed.join(", ")} failed (${elapsed}s)\n`);
        } else {
          sendStderr(`[qmd daemon] All warm in ${elapsed}s\n`);
        }
        return { ok: true, data: { elapsed: parseFloat(elapsed), loaded, failed } };
      }

      case "search": {
        const query = args.query as string;
        const limit = (args.limit as number) || 10;
        const collection = normalizeCollection(args.collection);
        const minScore = (args.minScore as number) || 0;

        const results = byScoreDesc(collectionScopes(collection).flatMap((scope) =>
          store.searchFTS(query, limit, scope)
        ))
          .filter((r: any) => r.score >= minScore)
          .slice(0, limit)
          .map((r: any) => {
            const { line, snippet } = extractSnippet(r.body || "", query, 300, r.chunkPos);
            return {
              docid: `#${r.docid}`,
              file: r.displayPath,
              title: r.title,
              score: Math.round(r.score * 100) / 100,
              context: store.getContextForFile(r.filepath),
              snippet,
              line,
            };
          });

        return { ok: true, data: { results, query } };
      }

      case "vsearch": {
        const query = args.query as string;
        const limit = (args.limit as number) || 10;
        const collection = normalizeCollection(args.collection);
        const minScore = (args.minScore as number) || 0.3;
        debug("daemon.vsearch", "start", { query, limit, collection, minScore });

        // Intercept stderr for progress
        const origWrite = process.stderr.write.bind(process.stderr);
        process.stderr.write = ((chunk: any, ...rest: any[]) => {
          sendStderr(typeof chunk === 'string' ? chunk : chunk.toString());
          return true;
        }) as any;

        try {
          sendStderr(`Expanding query...\n`);
          const expandStart = Date.now();

          // PERF: kick off the original query's vec search in parallel with
          // the LLM expansion. The original phrasing is always one of the
          // sub-searches anyway, and expandQuery uses the generate model
          // while searchVec uses the embed model — independent code paths
          // that can run concurrently without contention.
          //
          // When expandQuery is uncached, this hides ~1s of vec scan inside
          // the ~2.5s LLM call. When cached, original vec finishes first;
          // we still await both before merging to keep the merge consistent.
          const originalVecPromise = Promise.all(
            collectionScopes(collection).map(scope =>
              store.searchVec(query, DEFAULT_EMBED_MODEL, limit, scope)
            )
          );

          const queries = await store.expandQuery(query, DEFAULT_QUERY_MODEL);
          const vsExpandMs = Date.now() - expandStart;
          debug("daemon.vsearch", `expanded in ${vsExpandMs}ms`, {
            count: queries.length,
            types: queries.map(q => q.type),
            cached: vsExpandMs < 50,
          });

          const originalVecMs = Date.now() - expandStart;
          const originalVecScopes = await originalVecPromise;
          debug("daemon.vsearch", `original vec done`, {
            elapsedSinceStart: originalVecMs,
            hiddenInsideExpand: vsExpandMs >= originalVecMs,
            scopes: originalVecScopes.length,
            hits: originalVecScopes.reduce((sum, r) => sum + r.length, 0),
          });

          sendStderr(`Expanded to ${queries.length} queries, searching vectors...\n`);

          // Collect results — original results already done above.
          // Always include the original query alongside vec/hyde expansions —
          // the user-typed phrasing is often the best semantic match and the
          // LLM-generated alternatives can drift far from the source intent
          // (deterministic example: "mortality death existential meaning"
          // expanded to "overview of existential" which doesn't reach
          // kindle-highlights vectors, but the original query does).
          // Lex-typed expansions target BM25 in the hybrid path; skip them
          // in pure vsearch to avoid wasted vec scans.
          const allResults = new Map<string, any>();
          for (const scopeResults of originalVecScopes) {
            for (const r of scopeResults) {
              const existing = allResults.get(r.filepath);
              if (!existing || r.score > existing.score) {
                allResults.set(r.filepath, r);
              }
            }
          }

          const expansionTexts = queries
            .filter(q => typeof q === 'string' || q.type !== 'lex')
            .map(q => typeof q === 'string' ? q : q.query);
          for (const queryText of expansionTexts) {
            for (const scope of collectionScopes(collection)) {
              const vecResults = await store.searchVec(queryText, DEFAULT_EMBED_MODEL, limit, scope);
              for (const r of vecResults) {
                const existing = allResults.get(r.filepath);
                if (!existing || r.score > existing.score) {
                  allResults.set(r.filepath, r);
                }
              }
            }
          }

          // vsearch path returns vec-ranked results directly (no rerank step).
          // Use `qmd query` for the hybrid path which DOES rerank (see daemon.query).
          debug("daemon.vsearch", `found ${allResults.size} unique results`, {
            sub_searches: queries.length,
            unique_results: allResults.size,
            rerank: "skipped",
          });
          sendStderr(`Found ${allResults.size} unique results\n`);

          const results = Array.from(allResults.values())
            .sort((a: any, b: any) => b.score - a.score)
            .slice(0, limit)
            .filter((r: any) => r.score >= minScore)
            .map((r: any) => {
              const { line, snippet } = extractSnippet(r.body || "", query, 300);
              return {
                docid: `#${r.docid}`,
                file: r.displayPath,
                title: r.title,
                score: Math.round(r.score * 100) / 100,
                context: store.getContextForFile(r.filepath),
                snippet,
                line,
              };
            });

          return { ok: true, data: { results, query } };
        } catch (err: any) {
          const errMsg = err?.message || String(err);
          debug("daemon.vsearch", "ERROR", { error: errMsg, stack: err?.stack?.split("\n").slice(0, 3) });
          // On "database is locked", dump the active process list so we can
          // identify what's holding the write lock. Use require()'d child_process
          // since we already have it imported elsewhere in this file.
          if (errMsg.includes("database is locked")) {
            try {
              const { execSync } = await import("node:child_process");
              const pids = execSync("pgrep -af 'bun.*qmd|qmd' 2>/dev/null | head -20", { encoding: "utf-8" }).trim();
              debug("daemon.vsearch", "LOCK CONTEXT — processes that might hold the lock", {
                self_pid: process.pid,
                candidates: pids.split("\n").slice(0, 10),
              });
            } catch (_e) { /* ignore probe failure */ }
          }
          console.error(`[qmd daemon] vsearch error:`, err);
          return { ok: false, error: `vsearch failed: ${errMsg}` };
        } finally {
          process.stderr.write = origWrite;
        }
      }

      case "query": {
        const query = args.query as string;
        const limit = (args.limit as number) || 5;
        const collection = normalizeCollection(args.collection);
        const minScore = (args.minScore as number) || 0;
        debug("daemon.query", "start", { query, limit, collection, minScore });

        // Intercept stderr for progress
        const origWrite = process.stderr.write.bind(process.stderr);
        process.stderr.write = ((chunk: any, ...rest: any[]) => {
          sendStderr(typeof chunk === 'string' ? chunk : chunk.toString());
          return true;
        }) as any;

        try {
          // Replicate the querySearch pipeline from qmd.ts
          // Search each resolved collection directly. Prefix filters such as
          // "-c memory" expand to multiple small collections; global top-K
          // followed by post-filtering loses recall in that case.
          const scopes = collectionScopes(collection);
          const initialFts = byScoreDesc(scopes.flatMap((scope) =>
            store.searchFTS(query, 20, scope, { noNicheBoost: true })
          ));

          const hasVectors = !!store.db.prepare(
            `SELECT name FROM sqlite_master WHERE type='table' AND name='vectors_vec'`
          ).get();

          const topScore = initialFts[0]?.score ?? 0;
          const secondScore = initialFts[1]?.score ?? 0;
          const hasStrongSignal = initialFts.length > 0 && topScore >= 0.85 && (topScore - secondScore) >= 0.15;
          debug("daemon.query", "BM25 probe", { ftsResults: initialFts.length, topScore, secondScore, hasStrongSignal });

          let results: any[] = [];

          await withLLMSession(async (session) => {
            let ftsQueries: string[] = [query];
            let vectorQueries: string[] = [query];

            if (!hasStrongSignal) {
              const expandStart = Date.now();
              const queryables = await session.expandQuery(query);
              const qExpandMs = Date.now() - expandStart;
              debug("daemon.query", `expanded in ${qExpandMs}ms`, {
                count: queryables.length,
                types: queryables.map(q => q.type),
                cached: qExpandMs < 50,
              });
              for (const q of queryables) {
                if (q.type === 'lex' && q.text && q.text !== query) ftsQueries.push(q.text);
                else if ((q.type === 'vec' || q.type === 'hyde') && q.text && q.text !== query) vectorQueries.push(q.text);
              }
            }

            sendStderr(`Searching ${ftsQueries.length} lexical + ${vectorQueries.length} vector queries...\n`);

            const rankedLists: DaemonRankedResult[][] = [];
            const hashMap = new Map<string, string>();
            const searchPromises: Promise<void>[] = [];

            for (const q of ftsQueries) {
              const ftsResults = byScoreDesc(scopes.flatMap((scope) =>
                store.searchFTS(q, 20, scope, { noNicheBoost: true })
              ));
              if (ftsResults.length > 0) {
                rankedLists.push(ftsResults.map((r: any) => {
                  hashMap.set(r.filepath, r.hash);
                  return { file: r.filepath, displayPath: r.displayPath, title: r.title, score: r.score, hash: r.hash, docid: r.docid, body: r.body };
                }));
              }
            }

            if (hasVectors) {
              for (const q of vectorQueries) {
                searchPromises.push((async () => {
                  const vecResults: any[] = [];
                  for (const scope of scopes) {
                    vecResults.push(...await store.searchVec(q, DEFAULT_EMBED_MODEL, 20, scope, undefined, undefined, { noBM25Anchor: true }));
                  }
                  byScoreDesc(vecResults);
                  if (vecResults.length > 0) {
                    rankedLists.push(vecResults.map((r: any) => {
                      hashMap.set(r.filepath, r.hash);
                      return { file: r.filepath, displayPath: r.displayPath, title: r.title, score: r.score, hash: r.hash, docid: r.docid, body: r.body };
                    }));
                  }
                })());
              }
              await Promise.all(searchPromises);
            }

            if (rankedLists.length === 0) return;

            debug("daemon.query", "ranked lists", { lists: rankedLists.length, sizes: rankedLists.map(l => l.length) });
            const weights = rankedLists.map(() => 1.0);
            const fused = reciprocalRankFusion(rankedLists, weights) as DaemonRankedResult[];
            const RERANK_DOC_LIMIT = 40;
            const candidates = fused.slice(0, RERANK_DOC_LIMIT);
            debug("daemon.query", "RRF fusion", { fused: fused.length, candidates: candidates.length });

            // Prepare chunks for reranking
            const chunksToRerank = candidates.map((c: DaemonRankedResult) => {
              const body = c.body || "";
              return { file: c.file, text: body.slice(0, 3000), displayPath: c.displayPath, title: c.title, score: c.score, docid: c.docid };
            });

            debug("daemon.query", "reranking", { chunks: chunksToRerank.length });
            sendStderr(`Reranking ${chunksToRerank.length} documents...\n`);

            const rerankStart = Date.now();
            const rerankResult = await session.rerank(
              query,
              chunksToRerank.map((ch: any) => ({ file: ch.file, text: ch.text }))
            );

            debug("daemon.query", `rerank done in ${Date.now() - rerankStart}ms`, { resultCount: rerankResult.results.length });

            // Blend scores — session.rerank returns { results: [...], model: "..." }
            results = rerankResult.results
              .map((r: any) => {
                const candidate = chunksToRerank.find((c: any) => c.file === r.file);
                if (!candidate) return null;
                const rrfIdx = candidates.findIndex((c: any) => c.file === r.file);
                const rrfScore = rrfIdx >= 0 ? candidates[rrfIdx]!.score : 0;

                let rrfWeight: number;
                if (rrfIdx < 3) rrfWeight = 0.75;
                else if (rrfIdx < 10) rrfWeight = 0.60;
                else rrfWeight = 0.40;

                const blended = rrfWeight * rrfScore + (1 - rrfWeight) * r.score;

                return {
                  docid: `#${candidate.docid}`,
                  file: candidate.displayPath,
                  title: candidate.title,
                  score: Math.round(blended * 100) / 100,
                  context: store.getContextForFile(candidate.file),
                  snippet: extractSnippet(candidate.text, query, 300).snippet,
                };
              })
              .filter(Boolean)
              .sort((a: any, b: any) => b.score - a.score)
              .filter((r: any) => r.score >= minScore)
              .slice(0, limit);
          }, { maxDuration: 10 * 60 * 1000, name: 'daemonQuery' });

          return { ok: true, data: { results, query } };
        } finally {
          process.stderr.write = origWrite;
        }
      }

      case "shutdown": {
        console.error("[qmd daemon] Shutting down...");
        // Cleanup will happen in the finally block of startDaemonServer
        setTimeout(async () => {
          await disposeDefaultLlamaCpp();
          cleanupStaleFiles("shutdown-command-handler");
          process.exit(0);
        }, 100);
        return { ok: true, data: "Shutting down" };
      }

      default:
        return { ok: false, error: `Unknown command: ${command}` };
    }
    })();
    debug("daemon", `<<< ${command} done in ${Date.now() - cmdStart}ms`, { ok: result.ok });
    return result;
    } catch (err: any) {
      debug("daemon", `<<< ${command} ERROR in ${Date.now() - cmdStart}ms`, { error: err?.message || String(err) });
      throw err;
    }
  }

  // -------------------------------------------------------------------------
  // Unix socket server
  // -------------------------------------------------------------------------

  let clientCount = 0;
  const server = createServer((socket: Socket) => {
    clientCount++;
    const clientId = clientCount;
    debug("daemon.conn", `client connected (#${clientId})`);
    let buffer = "";

    socket.on("data", (chunk) => {
      buffer += chunk.toString();

      const newlineIdx = buffer.indexOf("\n");
      if (newlineIdx === -1) return;

      const line = buffer.slice(0, newlineIdx);
      buffer = buffer.slice(newlineIdx + 1);

      let parsed: { command: string; args: Record<string, unknown> };
      try {
        parsed = JSON.parse(line);
      } catch {
        socket.write(JSON.stringify({ ok: false, error: "Invalid JSON" }) + "\n");
        socket.end();
        return;
      }

      const sendStderr = (msg: string) => {
        try {
          socket.write(JSON.stringify({ type: "stderr", data: msg }) + "\n");
        } catch {}
      };

      handleCommand(parsed.command, parsed.args || {}, sendStderr)
        .then((result) => {
          try {
            socket.write(JSON.stringify(result) + "\n");
          } catch {}
          socket.end();
        })
        .catch((err) => {
          console.error(`[qmd daemon] Error handling ${parsed.command}:`, err);
          try {
            socket.write(JSON.stringify({ ok: false, error: String(err?.message || err) }) + "\n");
          } catch {}
          socket.end();
        });
    });

    socket.on("error", (err: NodeJS.ErrnoException) => {
      debug("daemon.conn", `client #${clientId} socket error`, { code: err.code, message: err.message });
    });
  });

  server.on("error", (err: NodeJS.ErrnoException) => {
    if (err.code === "EADDRINUSE") {
      // Socket file exists — try removing it and retrying once
      debug("daemon.socket", "EADDRINUSE, removing stale socket and retrying", { path: SOCKET_PATH });
      console.error(`[qmd daemon] Socket in use, removing stale socket and retrying...`);
      try { unlinkSync(SOCKET_PATH); } catch {}
      server.listen(SOCKET_PATH, () => {
        debug("daemon.socket", "listening after EADDRINUSE retry", { path: SOCKET_PATH });
        console.error(`[qmd daemon] Listening on ${SOCKET_PATH}`);
      });
    } else {
      debug("daemon.socket", "server error", { code: err.code, message: err.message });
      console.error(`[qmd daemon] Server error: ${err.message}`);
      cleanupStaleFiles(`server.error.${err.code}`);
      process.exit(1);
    }
  });

  server.listen(SOCKET_PATH, () => {
    debug("daemon.socket", "listening", { path: SOCKET_PATH });
    console.error(`[qmd daemon] Listening on ${SOCKET_PATH}`);
  });

  // Graceful shutdown on signals
  const shutdown = async (signal: string) => {
    const t0 = Date.now();
    debug("daemon.shutdown", `${signal} received, shutting down`, {
      signal,
      selfPid: process.pid,
      selfPpid: process.ppid,
      uptimeMs: Math.round(process.uptime() * 1000),
    });
    console.error(`[qmd daemon] ${signal} received, shutting down...`);
    server.close();
    await disposeDefaultLlamaCpp();
    cleanupStaleFiles(`shutdown.${signal}`);
    debug("daemon.shutdown", `done in ${Date.now() - t0}ms`);
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  // Keep process alive
  await new Promise(() => {});
}

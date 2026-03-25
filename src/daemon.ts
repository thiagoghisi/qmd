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

import { unlinkSync, existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { debug } from "./debug.js";
import { createServer, connect, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";

// =============================================================================
// Paths and constants
// =============================================================================

const RUNTIME_DIR = join(homedir(), ".cache", "qmd");
const SOCKET_PATH = join(RUNTIME_DIR, "daemon.sock");
const PID_FILE = join(RUNTIME_DIR, "daemon.pid");

export { SOCKET_PATH, PID_FILE, RUNTIME_DIR };

// =============================================================================
// Daemon client (used by CLI to check/connect to daemon)
// =============================================================================

/**
 * Check if a daemon is running and reachable.
 */
export function isDaemonRunning(): boolean {
  if (!existsSync(PID_FILE)) return false;

  try {
    const pid = parseInt(readFileSync(PID_FILE, "utf-8").trim(), 10);
    // Check if process is alive (signal 0 = just check)
    process.kill(pid, 0);
    // Process alive — check socket
    if (existsSync(SOCKET_PATH)) return true;
    // Process alive but socket missing — zombie daemon.
    // Kill it so a fresh start can recreate the socket.
    try { process.kill(pid, "SIGTERM"); } catch {}
    cleanupStaleFiles();
    return false;
  } catch {
    // Process not running - clean up stale files
    cleanupStaleFiles();
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
        cleanupStaleFiles();
        resolve({ ok: false, error: "Daemon not running" });
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

function cleanupStaleFiles(): void {
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

  // Clean up any stale socket
  cleanupStaleFiles();

  // Write PID file
  writeFileSync(PID_FILE, String(process.pid));

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

        await withLLMSession(async (session) => {
          // Load embed + generate in parallel
          const results = await Promise.allSettled([
            session.expandQuery("test warmup query").then(() => { loaded.push("generate"); }),
            session.embed("test warmup text").then(() => { loaded.push("embed"); }),
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

        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        if (failed.length > 0) {
          sendStderr(`[qmd daemon] Warmup partial: ${loaded.join(", ")} loaded; ${failed.join(", ")} failed (${elapsed}s)\n`);
        } else {
          sendStderr(`[qmd daemon] All models warm in ${elapsed}s\n`);
        }
        return { ok: true, data: { elapsed: parseFloat(elapsed), loaded, failed } };
      }

      case "search": {
        const query = args.query as string;
        const limit = (args.limit as number) || 10;
        const collection = normalizeCollection(args.collection);
        const minScore = (args.minScore as number) || 0;

        const results = store.searchFTS(query, limit)
          .filter((r: any) => collection.length === 0 || collection.includes(r.collectionName))
          .filter((r: any) => r.score >= minScore)
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
          const queries = await store.expandQuery(query, DEFAULT_QUERY_MODEL);
          debug("daemon.vsearch", `expanded in ${Date.now() - expandStart}ms`, { count: queries.length, types: queries.map(q => q.type) });
          sendStderr(`Expanded to ${queries.length} queries, searching vectors...\n`);

          // Collect results
          // Pass single collection name to searchVec for wider k when filtering
          const singleCollection = collection.length === 1 ? collection[0] : undefined;
          const allResults = new Map<string, any>();
          for (const q of queries) {
            const vecResults = await store.searchVec(q.text, DEFAULT_EMBED_MODEL, limit, singleCollection);
            for (const r of vecResults) {
              if (collection.length > 1 && !collection.includes(r.collectionName)) continue;
              const existing = allResults.get(r.filepath);
              if (!existing || r.score > existing.score) {
                allResults.set(r.filepath, r);
              }
            }
          }

          debug("daemon.vsearch", `found ${allResults.size} unique results`);
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
          debug("daemon.vsearch", "ERROR", { error: err?.message || String(err), stack: err?.stack?.split("\n").slice(0, 3) });
          console.error(`[qmd daemon] vsearch error:`, err);
          return { ok: false, error: `vsearch failed: ${err?.message || err}` };
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
          // Pass single collection to searchFTS for SQL-level filtering (100K → 725 docs)
          const singleCollectionFts = collection.length === 1 ? collection[0] : undefined;
          const initialFts = store.searchFTS(query, 20, singleCollectionFts)
            .filter((r: any) => collection.length === 0 || collection.includes(r.collectionName));

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
              debug("daemon.query", `expanded in ${Date.now() - expandStart}ms`, { count: queryables.length, types: queryables.map(q => q.type) });
              for (const q of queryables) {
                if (q.type === 'lex' && q.text && q.text !== query) ftsQueries.push(q.text);
                else if ((q.type === 'vec' || q.type === 'hyde') && q.text && q.text !== query) vectorQueries.push(q.text);
              }
            }

            sendStderr(`Searching ${ftsQueries.length} lexical + ${vectorQueries.length} vector queries...\n`);

            const rankedLists: any[][] = [];
            const hashMap = new Map<string, string>();
            const searchPromises: Promise<void>[] = [];

            for (const q of ftsQueries) {
              const ftsResults = store.searchFTS(q, 20, singleCollectionFts)
                .filter((r: any) => collection.length === 0 || collection.includes(r.collectionName));
              if (ftsResults.length > 0) {
                rankedLists.push(ftsResults.map((r: any) => {
                  hashMap.set(r.filepath, r.hash);
                  return { filepath: r.filepath, displayPath: r.displayPath, title: r.title, score: r.score, hash: r.hash, docid: r.docid, body: r.body };
                }));
              }
            }

            if (hasVectors) {
              const querySingleCollection = collection.length === 1 ? collection[0] : undefined;
              for (const q of vectorQueries) {
                searchPromises.push((async () => {
                  const vecResults = await store.searchVec(q, DEFAULT_EMBED_MODEL, 20, querySingleCollection)
                    .then((r: any[]) => r.filter((r: any) => collection.length <= 1 || collection.includes(r.collectionName)));
                  if (vecResults.length > 0) {
                    rankedLists.push(vecResults.map((r: any) => {
                      hashMap.set(r.filepath, r.hash);
                      return { filepath: r.filepath, displayPath: r.displayPath, title: r.title, score: r.score, hash: r.hash, docid: r.docid, body: r.body };
                    }));
                  }
                })());
              }
              await Promise.all(searchPromises);
            }

            if (rankedLists.length === 0) return;

            debug("daemon.query", "ranked lists", { lists: rankedLists.length, sizes: rankedLists.map(l => l.length) });
            const weights = rankedLists.map(() => 1.0);
            const fused = reciprocalRankFusion(rankedLists, weights);
            const RERANK_DOC_LIMIT = 40;
            const candidates = fused.slice(0, RERANK_DOC_LIMIT);
            debug("daemon.query", "RRF fusion", { fused: fused.length, candidates: candidates.length });

            // Prepare chunks for reranking
            const chunksToRerank = candidates.map((c: any) => {
              const body = c.body || "";
              return { file: c.filepath, text: body.slice(0, 3000), displayPath: c.displayPath, title: c.title, score: c.score, docid: c.docid };
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
                const rrfIdx = candidates.findIndex((c: any) => c.filepath === r.file);
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
          cleanupStaleFiles();
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
      cleanupStaleFiles();
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
    debug("daemon.shutdown", `${signal} received, shutting down`);
    console.error(`[qmd daemon] ${signal} received, shutting down...`);
    server.close();
    await disposeDefaultLlamaCpp();
    cleanupStaleFiles();
    debug("daemon.shutdown", `done in ${Date.now() - t0}ms`);
    process.exit(0);
  };

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  // Keep process alive
  await new Promise(() => {});
}

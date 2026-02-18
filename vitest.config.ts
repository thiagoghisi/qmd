import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 30000,
    include: ["test/**/*.test.ts"],
    // Disable file parallelism to prevent GPU memory exhaustion.
    // LLM integration tests (embed, rerank, expandQuery) share GPU memory
    // and can't run in parallel without OOM crashes on Metal.
    fileParallelism: false,
    // Global setup for GPU memory cleanup between test files
    setupFiles: ["test/vitest.setup.ts"],
  },
});

/**
 * Vitest global setup/teardown for GPU memory management.
 *
 * Metal GPU memory isn't automatically freed between test files.
 * This ensures LlamaCpp models are disposed after each file completes.
 */
import { afterAll } from "vitest";
import { disposeDefaultLlamaCpp } from "../src/llm.js";

afterAll(async () => {
  // Free GPU memory after each test file to prevent OOM in subsequent files
  await disposeDefaultLlamaCpp();
});

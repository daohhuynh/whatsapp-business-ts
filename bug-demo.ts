import { withResumableReconnect } from "./src/streaming/reconnect.ts";

// ════════════════════════════════════════════════════════════════════
// Bug 1 — deterministic regression test for missing idle timeout
//
// What this test proves
// ─────────────────────
// `src/streaming/reconnect.ts:44-58` defines `consumeStream`:
//
//   async function* consumeStream<T>(stream, state, opts) {
//     let receivedAtLeastOne = false;
//     for await (const event of stream) {     // <-- no idle timeout
//       if (!receivedAtLeastOne) { ... }
//       yield event;
//     }
//   }
//
// This `for await` has no idle timeout. If the underlying gRPC iterator
// stops yielding without throwing or ending (which happens during real
// network disruptions — wifi loss, NAT timeout, server cycling), this
// loop sits forever. No `yield`, no `throw`, no end. The reconnect
// machinery at line 156 only fires on throw or end, neither of which
// occurs. The consumer is silently deaf.
//
// The existing test suite (tests/unit/reconnect.test.ts) covers:
//   ✓ stream yields events normally
//   ✓ stream ends cleanly (return) → reconnect fires
//   ✗ stream hangs forever → no test exists ← THIS IS THE GAP
//
// This script fills the gap with a deterministic test:
//   1. Provide a stream that yields 2 events, then hangs.
//   2. Consume from the SDK's actual `withResumableReconnect`.
//   3. Measure the time gap after the hang begins.
//   4. After 8 seconds of silence with no recovery, the bug is proven.
//
// What a senior reviewer should do with this
// ──────────────────────────────────────────
// 1. Read the consume loop at reconnect.ts:51 — confirm no timeout.
// 2. Run this script — observe the hang is reproducible 100% of the time.
// 3. Add an idle timeout to consumeStream (see "Suggested Fix" at bottom).
// 4. Add this scenario to tests/unit/reconnect.test.ts.
// ════════════════════════════════════════════════════════════════════

const HANG_AFTER_EVENTS = 2;
const OBSERVATION_WINDOW_MS = 8000;

// The stream factory provided to the SDK. It yields a few events,
// then hangs on a never-resolving promise. This simulates what the
// real gRPC iterator does when its underlying TCP connection is in
// the "looks alive but no data flowing" state — which is exactly
// what the SDK is supposed to handle but doesn't.
let attempt = 0;
function createHangingStream(): AsyncIterable<{ readonly id: number }> {
  attempt += 1;
  const attemptNumber = attempt;
  return (async function* () {
    for (let i = 1; i <= HANG_AFTER_EVENTS; i += 1) {
      yield { id: i };
    }
    console.log(
      `[demo]   stream #${attemptNumber}: yielded ${HANG_AFTER_EVENTS} events, now hanging (no yield, no throw, no return)`
    );
    await new Promise<never>(() => {
      // intentionally never resolve — this is what a hung iterator looks like
    });
  })();
}

async function fetchMissed(_cursor: string): Promise<unknown[]> {
  return [];
}

function getCursor(): string | undefined {
  return undefined;
}

console.log("════════════════════════════════════════════════════════════════");
console.log("Bug 1 deterministic regression test");
console.log("Module under test: src/streaming/reconnect.ts (imported as-is)");
console.log("Scenario: upstream iterator hangs after yielding 2 events");
console.log(`Observation window: ${OBSERVATION_WINDOW_MS}ms after hang`);
console.log("════════════════════════════════════════════════════════════════");
console.log("");

// Track every event the SDK delivers to us, and the timestamps.
const eventsDelivered: { id: number; at: number }[] = [];
const reconnectAttempts: number[] = [];
const startTime = Date.now();

const stream = withResumableReconnect(
  createHangingStream,
  fetchMissed,
  getCursor,
  {
    initialDelay: 100,
    onReconnect: (attemptNum) => {
      const elapsedMs = Date.now() - startTime;
      reconnectAttempts.push(elapsedMs);
      console.log(`[demo] onReconnect callback fired — attempt=${attemptNum} at +${elapsedMs}ms`);
    },
  }
);

// Run the consumer with a deterministic observation window. If the SDK
// recovers within the window, the bug isn't reproducing. If it doesn't,
// the hang is observable and reproducible.
const consumerTask = (async () => {
  for await (const event of stream as AsyncIterable<{ readonly id: number }>) {
    const elapsedMs = Date.now() - startTime;
    eventsDelivered.push({ id: event.id, at: elapsedMs });
    console.log(`[demo] consumer received event id=${event.id} at +${elapsedMs}ms`);
  }
})();

const observationTimer = new Promise<"timeout">((resolve) => {
  setTimeout(() => resolve("timeout"), OBSERVATION_WINDOW_MS + 500);
});

const result = await Promise.race([
  consumerTask.then(() => "consumer-exited" as const),
  observationTimer,
]);

console.log("");
console.log("════════════════════════════════════════════════════════════════");
console.log("RESULT");
console.log("════════════════════════════════════════════════════════════════");
console.log(`Events delivered to consumer: ${eventsDelivered.length}`);
for (const e of eventsDelivered) {
  console.log(`  • id=${e.id} at +${e.at}ms`);
}
console.log(`Reconnect attempts fired: ${reconnectAttempts.length}`);
for (const t of reconnectAttempts) {
  console.log(`  • at +${t}ms`);
}
console.log(`Consumer state at end of observation window: ${result}`);

const hangStartMs = eventsDelivered.at(-1)?.at ?? 0;
const silenceDurationMs = Date.now() - startTime - hangStartMs;

if (result === "timeout") {
  console.log("");
  console.log("════════════════════════════════════════════════════════════════");
  console.log("BUG REPRODUCED");
  console.log("════════════════════════════════════════════════════════════════");
  console.log(`  ${HANG_AFTER_EVENTS} events delivered, then ${silenceDurationMs}ms of silence.`);
  console.log("  Zero reconnect attempts. Zero errors. Consumer is stuck.");
  console.log("");
  console.log("  Root cause: src/streaming/reconnect.ts:51");
  console.log("    `for await (const event of stream)` has no idle timeout.");
  console.log("");
  console.log("  Industry-standard fix: bound the iterator's next() with a");
  console.log("  timeout. The fix is shown in comments at the bottom of");
  console.log("  this file. ~10 lines added; existing reconnect machinery");
  console.log("  catches the thrown error and retries. No new public API.");
  console.log("");
  console.log("  This script will now hang forever (matching SDK behavior).");
  console.log("  Press Ctrl+C to exit.");
  console.log("════════════════════════════════════════════════════════════════");
  // Hold here so video viewers can observe the silence is permanent
  await new Promise<never>(() => {
    // intentionally never resolve
  });
} else {
  console.log("");
  console.log("Consumer exited before observation window ended. Bug not reproduced in this run.");
  process.exit(0);
}

// ════════════════════════════════════════════════════════════════════
// Suggested fix (~10 lines added to consumeStream)
// ────────────────────────────────────────────────────────────────────
//
// async function* consumeStream<T>(
//   stream: AsyncIterable<T>,
//   state: BackoffState,
//   opts: ResolvedOptions,
// ): AsyncGenerator<T> {
//   let receivedAtLeastOne = false;
//   const idleTimeoutMs = opts.idleTimeoutMs ?? 60_000;
//   const iterator = stream[Symbol.asyncIterator]();
//
//   while (true) {
//     const next = iterator.next();
//     const timeout = new Promise<never>((_, reject) =>
//       setTimeout(() => reject(new IdleTimeoutError(idleTimeoutMs)), idleTimeoutMs)
//     );
//     const result = await Promise.race([next, timeout]);
//     if (result.done) break;
//     if (!receivedAtLeastOne) {
//       receivedAtLeastOne = true;
//       state.consecutiveFailures = 0;
//       state.delay = opts.initialDelay;
//     }
//     yield result.value;
//   }
// }
//
// With this fix: the IdleTimeoutError throws → existing catch at
// reconnect.ts:97 fires → backoff and reconnect happen normally.
// The reconnect machinery already exists; we just need to give it a
// trigger.
// ════════════════════════════════════════════════════════════════════

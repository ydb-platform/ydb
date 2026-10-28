---
name: ydb-actor-system-debug
description: "Diagnose actor-runtime scheduling failures in ydb/library/actors: stuck mailboxes, lost wakeups, pool switching, and executor sleep races."
---

# Actor System Debugging

Use this workflow for runtime scheduling failures. When the investigation also
involves actor protocols, lifetime, or shutdown, also use
[actor development](../ydb-actor-development/SKILL.md).
Paths in code spans below are relative to the repository root.

## Source map

| Responsibility | Source |
|---|---|
| Compile-time logging thresholds | `ydb/library/actors/core/debug.h` |
| Mailbox push, lock, unlock, and requeue | `ydb/library/actors/core/mailbox_lockfree.h`, `ydb/library/actors/core/mailbox_lockfree.cpp` |
| Mailbox execution loop, per-mailbox event and time quantum, Tail execution, pool switching | `ydb/library/actors/core/executor_thread.cpp` |
| Activation scheduling entry point and stuck-actor statistics | `ydb/library/actors/core/executor_pool_base.cpp` |
| Activation publication and dedicated workers | `ydb/library/actors/core/executor_pool_basic.cpp` |
| IO pool activations | `ydb/library/actors/core/executor_pool_io.cpp` |
| Shared-worker eligibility, leases, notifications, and parking | `ydb/library/actors/core/executor_pool_shared.cpp` |
| Worker waiting implementation | `ydb/library/actors/core/executor_thread_ctx.cpp` |
| Pool quotas and thread limits | `ydb/library/actors/core/harmonizer/` |
| Executor and per-activity statistics | `ydb/library/actors/core/mon_stats.h` |
| Existing trace probes | `ydb/library/actors/core/probes.h` |

## Check application causes first

Before instrumenting the executor, exclude:

- A handler that blocks, sleeps, or runs long. It holds its worker and delays
  every actor on that mailbox. Check `CurrentActivationTime` and
  `ElapsedTicksByActivity` in `TExecutorThreadStats`; the stats collector
  publishes them per activity.
- An event that never reached the handler: the target had already detached,
  the ActorId was stale, or an `ActorWaitForEvent` waiter consumed the event
  before `StateFunc`.
- A Lazy send whose target activation was postponed until the current mailbox
  run finished.

With `MonitorStuckActors` enabled, `StuckActorsByActivity` counts actors whose
last received event is older than 30 seconds. Idle actors count too, so compare
it with the traffic the activity should receive.

If application checks do not explain the symptom, trace the runtime path to
locate the first incorrect transition.

## Reconstruct the failing path

1. Read the failing test and trace the event from sender to mailbox publication,
   activation dequeue, and handler dispatch. Identify the expected pool, owner
   pool, current pool, worker, and mailbox. Record the revision and test filter.
2. Check that the reproducer runs real executor threads. `TTestActorRuntimeBase`
   with `useRealThreads == false` replaces executor pools with
   `TExecutorPoolStub`, so its result says nothing about executor scheduling.
   With real threads it creates only basic and IO pools; for shared-worker
   failures, start from the setups in
   `ydb/library/actors/core/ut/actor_shared_threads.cpp`.
3. Read the active scheduling implementation and configuration before interpreting
   counters. In particular, `TBasicExecutorPool::GetSemaphore()` reads separate
   atomics when `EnableWaker` is enabled; those reads are not a coherent snapshot.
4. Locate the first transition whose expected effect is absent: publication,
   notification of an eligible worker, dequeue, or handler progress. Treat this
   as a hypothesis until a trace or reproducer excludes competing explanations.

For shared-worker failures, correlate activation credits, owner/adjacent/foreign
eligibility, lease acquisition and return, local/global notification consumption,
and working/local thread counters. Follow the actual worker's wait arguments:
a notification for another pool may not reach it. Trace counter changes and
notification rechecks through entry into `Wait()`, rather than inferring a lost
wakeup from one counter value.

## Add focused instrumentation

1. For suspected data races, run the focused test with `--sanitize=thread`. A
   clean run does not exclude atomic ordering or lost-wakeup bugs.
   `ydb/library/actors/core/tsan.supp` lists the known suppressions.
2. Find existing logging before adding it:

   ```bash
   rg -n 'ACTORLIB_DEBUG|EXECUTOR_.*_DEBUG' ydb/library/actors/core
   ```

3. If compile-time logging is needed, temporarily change `DebugLevel` in
   `ydb/library/actors/core/debug.h` to the smallest useful `EDebugLevel`.
   Levels are ordered: `ACTORLIB_DEBUG` writes every message at or below
   `DebugLevel` to `Cerr`. Check additional gates at the logging site: harmonizer
   logging also requires `DebugHarmonizerLevel` in
   `ydb/library/actors/core/harmonizer/debug.h`. Many core files include
   `debug.h`, so the change rebuilds much of the library.
4. Include pool, worker, and mailbox identities where applicable, plus the state
   transitions needed to test the hypothesis. Do not add blocking synchronization
   to make logs easier to read.
5. Compare with an uninstrumented baseline. Synchronous output can change thread
   interleavings; passing with logging does not establish correctness. When it
   masks the failure, inspect existing LWTrace probes and consider a minimal
   temporary probe set. Verify that the reproducer enables and collects the
   probes; adding `LWPROBE` alone does not produce a captured trace.

## Validation and cleanup

1. Follow active build instructions. Select the smallest relevant leaf target and
   one test filter. For actor-core failures, select the relevant suite from
   `ydb/library/actors/core/ut`: `ExecutorPoolSemaphore`, `BasicExecutorPool`,
   `ChangingThreadsCountInBasicExecutorPool`, `SharedThreads`, `LockFreeMailbox`,
   and `ExecutorPoolsTests`. For harmonizer failures, use
   `ydb/library/actors/core/harmonizer/ut`. Preserve the test stderr and trace
   artifacts rather than relying only on terminal output.
2. For a proposed fix, run the focused reproducer without instrumentation. Use
   repeated runs when needed to check the timing failure, and report the run
   count and any remaining uncertainty.
3. Remove only instrumentation added for this investigation. Preserve pre-existing
   user changes and restore the original debug threshold. Review:

   ```bash
   git diff -- ydb/library/actors/core/debug.h
   git diff -- ydb/library/actors/core
   git status --short
   ```

4. Report the revision, configuration, test filter, baseline and traced results,
   and the decisive transitions with artifact references. For investigations
   spanning sessions, keep a short record of observations, hypotheses, and
   counterevidence; recheck conclusions when the code or configuration changes.

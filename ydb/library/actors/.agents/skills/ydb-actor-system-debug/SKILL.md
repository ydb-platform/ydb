---
name: ydb-actor-system-debug
description: "Diagnose actor-runtime scheduling failures in ydb/library/actors: stuck mailboxes, lost wakeups, pool switching, and executor sleep races."
---

# Actor System Debugging

Use this workflow for runtime scheduling failures. For actor protocols, lifetime,
and shutdown, use [actor development](../ydb-actor-development/SKILL.md).
Paths in code spans below are relative to the repository root.

## Source map

| Responsibility | Source |
|---|---|
| Compile-time logging thresholds | `ydb/library/actors/core/debug.h` |
| Activation publication and dedicated workers | `ydb/library/actors/core/executor_pool_basic.cpp` |
| Shared-worker eligibility, leases, notifications, and parking | `ydb/library/actors/core/executor_pool_shared.cpp` |
| Worker waiting implementation | `ydb/library/actors/core/executor_thread_ctx.cpp` |
| Pool quotas and thread limits | `ydb/library/actors/core/harmonizer/` |
| Existing trace probes | `ydb/library/actors/core/probes.h` |

## Reconstruct the failing path

1. Read the failing test and trace the event from sender to mailbox publication,
   activation dequeue, and handler dispatch. Identify the expected pool, owner
   pool, current pool, worker, and mailbox. Record the revision and test filter.
2. Determine whether the test runs real executor threads or a simulated runtime.
   Use [testing actors](../../../../../docs/en/core/contributor/actor-system/testing.md)
   to choose a fixture that exercises the failing scheduling path.
3. Read the active scheduling implementation and configuration before interpreting
   counters. In particular, `GetSemaphore()` reads separate atomics when
   `EnableWaker` is enabled; those reads are not a coherent snapshot.
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

1. Find existing logging before adding it:

   ```bash
   rg -n 'ACTORLIB_DEBUG|EXECUTOR_.*_DEBUG' ydb/library/actors/core
   ```

2. If compile-time logging is needed, temporarily change `DebugLevel` in
   `ydb/library/actors/core/debug.h` to the smallest useful `EDebugLevel`.
   `ACTORLIB_DEBUG` enables levels up to that threshold and writes to `Cerr`.
   `Activation` includes earlier enum levels; `Event` also enables event
   diagnostics. Check additional gates at the logging site: harmonizer logging
   also requires `DebugHarmonizerLevel` in
   `ydb/library/actors/core/harmonizer/debug.h`. This header change requires rebuilding
   affected code.
3. Include pool, worker, and mailbox identities where applicable, plus the state
   transitions needed to test the hypothesis. Do not add blocking synchronization
   to make logs easier to read.
4. Compare with an uninstrumented baseline. Synchronous output can change thread
   interleavings; passing with logging does not establish correctness. When it
   masks the failure, inspect existing LWTrace probes and consider a minimal
   temporary probe set. Verify that the reproducer enables and collects the
   probes; adding `LWPROBE` alone does not produce a captured trace.

## Validation and cleanup

1. Follow active build instructions, including remote execution when required.
   Select the smallest relevant leaf target and one test filter; for actor-core
   failures, start with `ydb/library/actors/core/ut`. Preserve the test stderr and
   trace artifacts rather than relying only on terminal output.
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

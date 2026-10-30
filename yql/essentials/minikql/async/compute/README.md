# Resumable MiniKQL row computation (D2/D3)

This target builds on the D1 operation registry. It provides the internal YQL to
MiniKQL projection path and an ordered async Stream operator. It does **not**
register an SQL function, enable an engine mode, or migrate the WASM stack.
Engine/task-runner bridges and user-facing discovery are D6 work.

## Registration and compilation

The host populates `TTypeAnnotationContext::AsyncFunctions` with ordinary input
and result types, a provider name, operation and maximum encoded response size.
One argument can be a Struct/Tuple containing multiple SQL arguments. No native
UDF ABI or scalar `IBoxedValue::Run` method changes. Builtins can use the same
internal mechanism without registering a UDF module.

Internal YQL `AsyncInvoke(name, argument)` resolves its type from that catalogue;
`AsyncMap(streamOrFlow, rowLambda)` is the supported containing context. The host
sets `TMkqlBuildContext::AsyncTypes` to the same catalogue when compiling. A call
outside explicit AsyncMap lowering fails compilation. SQL/provider rewriting of
SELECT, existing Map, UDAF or other operators is intentionally not enabled here.

`GetAsyncExpressionEffects` propagates MaySuspend and external effects through
expressions and lambdas. Every async occurrence is conservatively effectful:
CSE cannot merge separate occurrences, purity analysis cannot classify them as
pure isolated computation, and compiler memoization does not hoist them out of
an invocation. Explicitly sharing one expression node shares its saved result
within that invocation. It is executed again for the next input row. No replay
safety or idempotence is inferred from these effects.

The lowerer produces a versioned program embedded in an ordinary serializable
MiniKQL callable. Its slots reference typed synchronous fragments, arguments,
branch operands and provider/operation identifiers, never callbacks or secrets.
An AsyncInvoke which reaches a scalar computation factory has no sync fallback.

Supported expressions are Data, Optional, Struct and Tuple values, direct row
arguments, constructors, a closed set of strict scalar primitives, If
(including nested If for CASE and an empty optional condition), and Unwrap with
a lazy diagnostic operand. The internal program uses version 2 for this explicit
Unwrap instruction; older versions are rejected during preparation. Unknown, lazy,
higher-order, Resource, list and block contexts fail preparation. This deliberate
subset avoids changing their evaluation semantics before explicit lowering.

## Frame execution

Each row owns a frame with saved values, an explicit instruction stack, selected
branch dependencies and at most one submitted operation. The C++ stack is not
preserved. Synchronous fragments bind their inputs from saved slots before
calling existing computation nodes. A resumed frame never re-evaluates a saved
volatile expression or submits the same pending call twice. Only the selected
If branch is visited; a NULL condition visits neither. Unwrap evaluates its
diagnostic, including any async calls, only after finding an empty optional.
The original MiniKQL fragment retains optional representation and error position.

AsyncMap admits a bounded window (default 16, maximum 1024) of independent frames
and returns output in input order. Other rows can progress while an earlier row
waits. Flow adapts through existing FromFlow/ToFlow Yield semantics. A scalar
GetValue result is never overloaded with a Pending sentinel.

Provider arguments/results use the MiniKQL `TValuePacker` codec for the declared
signature. Packing, unpacking, frame destruction and graph evaluation happen on
the owner with its allocator bound. A provider callback handles owned bytes only.
Provider errors fail the stream, cancel other outstanding frames, and cannot be
retried by fetching that failed stream again. Error payloads are not logged.

## Owner turn and wakeups

Create `TExecutionServices` for one graph execution, register providers, then put
it in `TComputationOptsFull::AsyncServices` when cloning the pattern. Services are
not held by the cached pattern. Missing services/providers fail before reading
input. The enqueue callback must use lifetime-safe, generation-aware delivery;
an already copied wakeup may arrive after shutdown, as in D1.

At the **start** of every owner compute event, including initial, source and timer
events, call `BeginTurn()`. It clears the notification gate before draining
results and replenishes both the instruction-step quantum and a cooperative thread
CPU budget (`CpuBudgetUs`, default 1000 microseconds, zero disables it). The clock
is injectable for deterministic tests. Call `EndTurn()` before leaving the owner
event so CPU metrics include its tail and exclude other tasks between events. This ordering
allows another event to be posted while the current one is still running.

- Runnable work exhausting its quantum explicitly requests a coalesced rerun
  before returning Yield, even after the last external response.
- Waiting frames with no local work return Yield without self-notification.
- Inline completion and completion racing drain use D1's atomic drain/rearm.
- Admission waits retry on a later owner event, including physical-cleanup credit
  notifications. An impossible request with no tracked operations fails.
- All streams sharing services share the quantum and result dispatcher.
- A source fetch is charged once until it returns a row or EOF. Its charge is
  retained across Yield, so nested AsyncMaps can advance even with a one-step
  shared quantum. Waiting still requires an owner event to retry the source.

There is no thread, timer, sleep or polling loop in this target. Step budgeting
is cooperative between synchronous fragments, not preemption of arbitrary C++.
D3 adds an owner-driven scheduler with optional native batch capability, provider
round-robin and per-stream frame round-robin. A frame already doing local work
advances before more input is read; pending independent operations can still fill
the bounded window. `Flush()` sends full and partial compatible batches at the
available-work/CPU Yield boundary. A capacity wait never requests a budget rerun.
The scheduler limits physical in-flight items until cleanup, separately from
D1's bound on all tracked entries and encoded payload bytes.

A memory quota accounts for queued/in-flight/ready buffers (one shared lease),
continuation metadata, and the graph allocator's retained pages. Retained page
usage is reconciled at Fetch/step boundaries; fragments and source Fetch remain
cooperative, so temporary allocations inside arbitrary synchronous code require
the engine allocator's own guard. Providers borrow the accepted request's buffer
reservation and must reserve additional private buffers explicitly. The quota
can be shared by owner contexts; actual task/node quota-manager adapters and
allocator-guard wiring remain D6. See [scheduler contract](../SCHEDULER.md) for
ownership, wakeup, accounting and metrics details.

## Unsupported execution modes

The pattern must use `OptLLVM=OFF`; D2 rejects codegen rather than generating a
synchronous wrapper. Graph SaveGraphState/LoadGraphState explicitly reject async
computation until the D4 checkpoint/replay contract exists. Do not enable this
path in a checkpointed engine before that integration.

## Tests

Run from this target's `ut` directory:

```sh
../../../../../../ya make --build relwithdebinfo -tA
```

The suite executes real MiniKQL graphs with a controlled native byte provider,
including YQL typing/CSE/lowering, serialized Flow plans, branch laziness, shared
and distinct occurrences, volatile values across interleaved frames, errors,
cancellation, cleanup admission wakeups and 200 rows. A one-step quantum test
finishes after the last external event using only runtime reruns; a separate test
requires a waiting graph to remain idle. Nested Stream/Flow maps are checked with
a one-step quantum, deferred replies and 200 rows; Unwrap covers skipped and
deferred diagnostics, nested optionals and serialized plans. These are deterministic
owner-event queue tests, not yet FQ/KQP actor integration tests (D6).

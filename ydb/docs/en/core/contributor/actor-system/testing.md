# Testing Actors

Actor tests control message delivery and lifecycle transitions to check protocol behavior. Select the narrowest existing fixture that models the contract you are changing. The [Actor System Guide](index.md) describes the common invariants; storage actors also need their component's persistence and recovery tests.

## Test Entry Points

| Change | Test target or fixture |
|---|---|
| Actor core, dispatch, registration, death | `ydb/library/actors/core/ut` |
| Stackless frames, waiters, cancellation | `ydb/library/actors/async/ut` |
| Interconnect behavior | `ydb/library/actors/interconnect/ut` |
| Generic actor test runtime | [testlib/test_runtime.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/testlib/test_runtime.h) |
| YDB tablet/component integration | Existing component fixture, often based on [core/testlib/test_client.h](https://github.com/ydb-platform/ydb/blob/main/ydb/core/testlib/test_client.h) |

Use edge actors to observe replies and the fixture's dispatch/observer mechanisms to delay or intercept events. Establish the causal point being tested explicitly instead of relying on a wall-clock sleep. Check the fixture's scheduling mode: simulated-time tests do not exercise every real executor or foreign-thread race.

For a changed request protocol, cover the successful response and relevant failure transitions: late or duplicate replies, timeout followed by completion, session replacement, and shutdown with work outstanding. For persistence, reboot through the component's durable state and verify externally observable recovery. Test forced teardown separately when cleanup can run without actor TLS.

Build and test invocation is described in [Ya Make](../build-ya.md); apply the active workspace or personal build instructions to the selected targets.

## Review checklist

- Is every actor-owned field mutated only from serialized actor execution?
- Can any System/User turn block, spin, do slow I/O, or hold a contended lock?
- Is blocking I/O on the IO dispatcher or isolated on IOPoolId?
- Does the actor set ActorName / activity type before Register?
- Does every registered raw pointer become runtime-owned immediately?
- Is Bootstrap's receiving state installed before return/first suspension?
- Do top-level coroutine handlers take all explicit parameters by value?
- Are event payload and TActorContext lifetimes respected across suspension?
- Does shutdown call PassAway once and tolerate events while tasks drain?
- Can every long wait be cancelled, timed out, or force-destroyed safely?
- Does request/reply echo a unique cookie (not 0 for concurrent waits) and
  tolerate stale replies?
- Does a remote protocol handle timeout, duplicate/ambiguous outcome, and a new
  Interconnect session?
- Are large events serializable, bounded, and released promptly?
- Are ServiceId mappings and aliases explicitly maintained?

## Coroutine Review Checklist

- Is this meant to be a root task, or should it return `async<T>` and be awaited?
- Do all root explicit parameters have non-reference types?
- Does coroutine Bootstrap Become before its first suspension?
- Is every `async<T>` consumed directly by co_await or by a library combinator
  that is itself the direct co_await operand?
- Do lambda closures and every borrowed reference outlive the full await?
- Is large incoming event data released before long suspension?
- Is each ActorWaitForEvent cookie unique among live waits (not 0), and are
  late replies handled by StateFunc?
- After each suspension, are actor state, pointers, generations, and shutdown
  status revalidated?
- Can cancellation reach every potentially infinite wait?
- Is a timeout treated as cooperative and externally ambiguous?
- Does PassAway switch to a dying state and return/co_return immediately?
- Does every RAII destructor remain safe during forced teardown without TLS?
- Is TAsyncCancellationScope.Cancel called explicitly when ownership ends?
- Do task-group children avoid borrowing locals that may die before children?
- Are actor-aware primitives touched only from the owning actor activation?
- For off-thread completion, is there exactly one bridge/event wakeup and a
  system-shutdown lifetime plan?

## Coroutine Source and Test Map

Header paths below are relative to `ydb/library/actors/async/`; unit tests are
in its `ut/` directory. `core/actor.cpp` is relative to `ydb/library/actors/`.

| Topic | Source | Behavioral tests |
|---|---|---|
| Root/nested promises and cancellation transport | async.h, async.cpp | async_ut.cpp |
| Event-by-cookie wait | wait_for_event.h | wait_for_event_ut.cpp |
| Actor-local continuation | continuation.h | continuation_ut.cpp |
| Actor-local notification | event.h | event_ut.cpp |
| Sleep/yield | sleep.h, yield.h | sleep_ut.cpp |
| Timeout/deadline | timeout.h | timeout_ut.cpp |
| Scopes/interception | cancellation.h | cancellation_ut.cpp |
| Structured concurrency | task_group.h | task_group_ut.cpp |
| Low-priority scheduling | low_priority.h | low_priority_ut.cpp |
| Root task/death integration | core/actor.cpp | async unit tests |
| Generic off-thread bridge | async.cpp, async.h | async_ut.cpp generic-awaiter tests |

Representative production patterns worth reading critically:

- ydb/core/tx/datashard/datashard__lock_rows.cpp: keyed request state,
  WithCurrentHandler, Y_DEFER cleanup, duplicate cancellation, and state
  re-lookup after suspension;
- ydb/core/tablet/tablet_resolver.cpp: actor-local events, cancellation scopes,
  keyed background loops, and teardown guards;
- ydb/core/kqp/node_service/kqp_query_control_plane.cpp: value-parameter async event
  handler and ActorWaitForEvent.

The tests and library source define the contract; production call sites are
examples, not authority.

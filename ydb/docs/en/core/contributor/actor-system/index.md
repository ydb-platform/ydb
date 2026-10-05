# Actor System Guide

The NActors actor system runs C++ objects by delivering messages to their mailboxes. This guide describes the contracts used to implement actors throughout {{ ydb-short-name }}, including tablets. Tablet persistence, transactions, and recovery add component-specific rules on top of these actor contracts.

Read the pages needed for your change:

| Task | Reference |
|---|---|
| Construct, register, and dispatch an actor; choose a pool | [Actor Lifecycle](actor-lifecycle.md) |
| Send, forward, schedule, or exchange remote messages | [Events and Messaging](events-and-messaging.md) |
| Handle nondelivery, cancellation, and actor death | [Failure and Shutdown](failure-and-shutdown.md) |
| Write a stackless actor handler | [Coroutine Actors](coroutine-actors.md) |
| Choose a wait, deadline, cancellation scope, or task group | [Coroutine Primitives](coroutine-primitives.md) |
| Integrate callbacks, custom awaiters, or keyed background work | [Coroutine Integration](coroutine-integration.md) |
| Select tests and review invariants | [Testing Actors](testing.md) |

## Mental model

| Object | Meaning |
|---|---|
| Actor | An IActor-derived C++ object with private state and one current StateFunc. |
| ActorId | The address assigned when an actor is registered. |
| Event | An IEventBase payload owned by an IEventHandle envelope. |
| Mailbox | A multi-producer event queue that serializes execution for its attached actors. |
| Executor pool | Scheduling and CPU capacity used to run mailbox activations. |
| Activation | A request for a worker to drain some work from a mailbox. |
| ServiceId | A stable local name mapped to a current ActorId. |
| Interconnect session | One incarnation of communication between two nodes. |

The basic turn is:

1. a sender transfers an event handle to the actor system;
2. the recipient mailbox is activated;
3. one worker finds the recipient actor and calls IActor::Receive;
4. Receive dispatches in this order:
   - if FlagSystemMessage is set, a runtime switch (TEvResumeRunnable,
     TEvCheckActorLiveness); any other type with that bit is ignored and never
     reaches user code;
   - else if a stackless ActorWaitForEvent waiter matches cookie and type, it
     consumes the event;
   - else the current StateFunc runs;
5. Receive's TActorRunnableQueue destructor then drains runnables scheduled
   during this turn (no extra mailbox event). Only then may the mailbox process
   another event. TEvResumeRunnable is a later mailbox hop used when resume is
   not already on this activation.

## Invariants to design around

1. **A mailbox executes serially.** No two handlers attached to the same
   mailbox run at once. The mailbox may run on different worker threads over
   time. Actors on different mailboxes may run concurrently.

2. **Serialization is not a global order.** A mailbox consumes its queue order,
   but sends racing from different producers have no useful relative order.
   Do not infer causality from independent senders. Interconnect channels and
   session replacement add further ordering boundaries.

3. **Scheduling is cooperative.** A running handler is never preempted at an
   executor quantum boundary. Quantum checks happen between events. A long or
   blocking handler monopolizes one worker and all actors on its mailbox; enough
   such handlers exhaust a pool. Never sleep, spin, wait on a future, perform
   slow synchronous I/O, or hold a contended lock in a System/User handler.
   Blocking work belongs on the IO dispatcher or an IO-pool actor (see pools).

4. **Actor-owned state normally needs no lock.** Only mutate it from that
   actor's turns. Shared memory is safe only under an explicit lifetime and
   synchronization design; same-mailbox actors are the usual lock-free sharing
   case. A mutex does not make blocking an executor worker harmless.

5. **Activation context is turn-scoped.** TlsActivationContext, IActor::Send,
   SelfId().Send, TActivationContext methods, and most actor-aware async
   primitives are valid only while actor code is executing. Never retain a
   TActorContext across a handler return or co_await: it contains references to
   the worker and mailbox of that activation. A non-actor thread must use a
   TActorSystem pointer.

6. **Sending transfers ownership.** After Send, Schedule, Register, or Forward,
   assume the supplied pointer or handle has been consumed, including when a
   boolean result is false. During a handler, ev owns its payload; a pointer
   returned by ev->Get is valid only while that handle still owns it. Copy data
   that must outlive the turn, or move/Release/Forward ownership deliberately.

7. **Delivery is not processing or success.** A successful Send means accepted
   for routing, not handled. Even TEvUndelivered only describes some forms of
   nondelivery; it cannot prove that application work did or did not happen.
   Protocols that need an outcome use request/reply, a deadline, and usually an
   idempotency key.

8. **Actor death is explicit and one-shot.** A registered actor is runtime
   owned. It calls PassAway exactly once; nobody deletes it directly.
   PassAway may defer detachment while stackless top-level tasks cancel. During
   that interval later events can still reach StateFunc.

9. **Registration parent is metadata, not supervision.** It is passed to
   Registered/AfterRegister and is the sender of a bootstrapped actor's
   Bootstrap event. Parent and child do not automatically share lifetime.

10. **ActorId is an ephemeral address.** It is not a durable identity across
    process restart. Its debug string is intentionally not a lossless
    serialization.

11. **Exceptions must end at a deliberate boundary.** Synchronous exceptions
    escape Receive unless an IActorExceptionHandler accepts them. An uncaught
    top-level coroutine exception takes the same handler path and otherwise
    terminates. Catch expected failures inside the protocol.

12. **Cross-node messages are not durable or exactly-once.** A session failure
    can make an in-flight outcome ambiguous. Reconnect/continuation behavior is
    a transport optimization, not an application guarantee.


## Runtime Source Map

The contracts above describe application behavior. For runtime changes, inspect the implementation and its tests:

| Topic | Source under `ydb/library/actors/` |
|---|---|
| Receive, PassAway, actor tasks | [core/actor.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/actor.cpp), [core/actor.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/actor.h) |
| Bootstrap and dispatch | [core/actor_bootstrapped.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/actor_bootstrapped.h), [core/hfunc.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/hfunc.h) |
| Envelopes and wire payloads | [core/event.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/event.h), [core/event_pb.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/event_pb.h) |
| Registration, services, routing | [core/actorsystem.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/actorsystem.cpp), [core/executor_pool_base.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/executor_pool_base.cpp) |
| Mailbox scheduling | [core/executor_thread.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/executor_thread.cpp), [core/mailbox_lockfree.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/mailbox_lockfree.h) |
| Blocking I/O offload | [core/io_dispatcher.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/io_dispatcher.h), [core/invoke.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/core/invoke.h) |
| Remote transport | [interconnect/](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors/interconnect) |
| Stackless coroutines | [async/](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors/async) |

The source and behavioral tests take precedence when implementation and prose diverge. Update the relevant contract page when changing an observable runtime guarantee.

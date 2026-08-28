# YDB actor system (NActors)

This is a practical contract for writing actors in the current YDB tree. It
separates guarantees that application code may rely on from executor and
Interconnect implementation details. Stackless coroutine details are in
[Stackless actor coroutines](#stackless-actor-coroutines) below.

Important headers:

- library/actors/core/actor.h and actor_bootstrapped.h
- library/actors/core/event.h, event_local.h, and event_pb.h
- library/actors/core/events.h and hfunc.h
- library/actors/core/actorsystem.h
- library/actors/async/async.h for stackless handlers

A YDB node process normally owns a TActorSystem. Local messages pass C++ event
objects by ownership transfer. A message to another node is serialized and
carried by Interconnect.

---

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

---

## A minimal actor

~~~cpp
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

using namespace NActors;

enum EEv {
    EvRequest = EventSpaceBegin(TEvents::ES_PRIVATE),
    EvResponse,
};

struct TEvRequest : TEventLocal<TEvRequest, EvRequest> {
    TString Value;
    explicit TEvRequest(TString value)
        : Value(std::move(value))
    {}
};

struct TEvResponse : TEventLocal<TEvResponse, EvResponse> {
    TString Value;
    explicit TEvResponse(TString value)
        : Value(std::move(value))
    {}
};

class TExampleActor final : public TActorBootstrapped<TExampleActor> {
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    void Handle(TEvRequest::TPtr& ev) {
        Send(ev->Sender, new TEvResponse(ev->Get()->Value), 0, ev->Cookie);
    }

    void BeginShutdown() {
        Become(&TThis::StateDying);
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvRequest, Handle);
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway may be waiting for coroutine tasks.
    }
};
~~~

TEvRequest and TEvResponse above are local-only. Use a serializable event for a
message that may cross nodes.

## Construction, bootstrap, and state dispatch

TActor<TDerived> takes an initial state function and begins waiting for events.
TActorBootstrapped<TDerived> installs a bootstrap state; registration injects a
Bootstrap event and dispatches to one of the supported Bootstrap overloads.

The constructor also records an activity type used in monitoring counters.
Defaults, in order: TDerived::ActorName (a static string), else
TDerived::ActorActivityType() (an enum), else the C++ type name. You may pass a
string or enum to the constructor, or call SetActivityType, but only before
Register: SetActivityType throws if the actor already has an ActorId.

Bootstrap must establish the state that will receive later events. For a
synchronous Bootstrap, call Become before returning. For a coroutine Bootstrap,
call Become before its first suspension, because its caller regains control at
that suspension.

The state-function signature is:

~~~cpp
void State(TAutoPtr<IEventHandle>& ev);
~~~

Become changes the actor-global current state. It does not affect only the
current request or coroutine. Consequently, overlapping coroutine handlers can
observe one another's Become calls.

Dispatch macros from hfunc.h are convenience switch cases:

| Macro | Called signature |
|---|---|
| hFunc(TEv, Handle) | Handle(TEv::TPtr&) |
| HFunc(TEv, Handle) | Handle(TEv::TPtr&, const TActorContext&) |
| sFunc / SFunc | Handle() / Handle(ctx), ignoring the payload |
| cFunc / CFunc | Dispatch by numeric event type, without a typed pointer |
| fFunc / FFunc | Handle(ev) / Handle(ev, ctx), using the raw handle |
| IgnoreFunc(TEv) | Drop that event |

STRICT_STFUNC diagnoses an unknown type with Y_DEBUG_ABORT_UNLESS in debug
builds. It is a debugging aid, not a release-build protocol. A plain STFUNC can
implement its own default behavior. Dispatch on GetTypeRewrite, as the macros
do, because forwarding and Interconnect may rewrite the effective type and
recipient.

### Coroutine handlers have a different signature

A top-level stackless coroutine is an IActor non-static member returning void.
Every explicit argument must be passed **by value**:

~~~cpp
void HandleAsync(TEvRequest::TPtr ev) {
    // ...
    co_await Something();
}

STRICT_STFUNC(StateWork,
    hFunc(TEvRequest, HandleAsync);
)
~~~

hFunc passes an lvalue TAutoPtr, whose copy-like operation transfers ownership
into the by-value coroutine parameter. A reference parameter does not select
the actor coroutine promise and fails to compile. See
[Stackless actor coroutines](#stackless-actor-coroutines) for the complete
contract.

## Registration and ownership

From an actor turn:

~~~cpp
TActorId child = Register(new TChild);
TActorId other = Register(
    new TChild,
    TMailboxType::HTSwap,
    AppData()->UserPoolId);
TActorId colocated = RegisterWithSameMailbox(new THelper);
~~~

From code with a TActorSystem pointer:

~~~cpp
TActorId id = actorSystem->Register(
    new TExampleActor,
    TMailboxType::HTSwap,
    poolId);
~~~

Registration transfers actor ownership immediately. Do not dereference the raw
pointer afterward. The runtime attaches the actor to a mailbox, assigns an
ActorId, and calls Registered; the default Registered calls AfterRegister and
sends the handle it returns, which is how a bootstrapped actor gets its
Bootstrap event. Overriding Registered without calling the base skips both.
Those registration callbacks must not assume they are an activation of the new
actor; do activation-dependent work from Bootstrap or a later event.

RegisterWithSameMailbox is valid from an actor activation. The actors then
serialize on the same mailbox and may intentionally share state, but cannot run
in parallel. IActor::InvokeOtherActor temporarily changes activation identity
for a direct same-mailbox call. It must be a wholly synchronous call: never let
it span co_await, and never call an async member on another actor through a raw
pointer.

The mailbox-type argument remains in the API for compatibility. The current
built-in executor registration path uses the lock-free intrusive mailbox and
does not select among the historical TMailboxType values. Do not tune
Simple/HTSwap/ReadAsFilled/etc. without first verifying the actual executor
implementation in use.

## Events and envelopes

An IEventHandle contains:

- original and rewritten type/recipient;
- Sender and Recipient ActorIds;
- a 64-bit Cookie;
- flags and a 12-bit Interconnect channel number;
- an in-process IEventBase or serialized data;
- trace and, for received remote events, the InterconnectSession ActorId.

Cookie is uninterpreted by the runtime except where a particular helper
documents otherwise. A common request/reply convention is to copy ev->Cookie
to the reply, but this is part of that protocol, not automatic behavior. Send's
cookie argument defaults to 0. Treat 0 as colliding whenever several inflight
waits can share a type: ActorWaitForEvent and TrackDelivery notifications key
on cookie, so generate a unique non-zero value per live wait.

Event type numbers are partitioned into 65,536-value spaces. Allocate a unique
range in the appropriate assigned event space for a component protocol.
TEvents::ES_PRIVATE is intentionally reused by actor-private/local protocols
and tests; it is not globally unique, so values that may reach the same
dispatcher must still be unambiguous.

### Payload classes

- TEventLocal<T, Type> is an in-process C++ payload and is not serializable.
- TEventPB<T, Proto, Type> is the normal protobuf-backed wire event. Binary
  blobs that should not live in the protobuf go beside it: AddPayload(TRope)
  returns an index; store that index in the proto; the peer reads GetPayload(i)
  / GetPayloadCount(). Do not stuff TRcBuf into protobuf bytes fields when you
  need zero-copy. The wire encoding is an implementation detail (currently
  marker 0x06, payload sizes, blobs, then the proto; 0x07 is a legacy layout
  accepted on parse only).
- TEventSimpleNonLocal<T, Type> is serializable but has no derived payload.
  Adding fields to a derived class does not put those fields on the wire.
  TEvPoison uses this and may be sent to another node.
- Flat/rope event APIs are for specialized zero-copy layouts.

Do not accidentally send a local event to another node. Interconnect's default
maximum serialized event size is 140 MiB, including serialization overhead;
configuration may change or lower it. An oversized wire event terminates the
whole session rather than merely dropping that event. Prefer chunked or
blob-oriented protocols for large data.

### Send flags

The commonly relevant low bits are:

| Flag | Contract |
|---|---|
| FlagTrackDelivery | Ask routing to report supported nondelivery cases. It is not an acknowledgement. |
| FlagForwardOnNondelivery | Route a local nondelivery using the handle's forward target; advanced use. |
| FlagSubscribeOnSession | For a remote send, subscribe Sender to that session incarnation. |
| FlagGenerateUnsureUndelivered | With FlagTrackDelivery, also report ambiguous Interconnect in-flight events as Unsure; no effect alone. |
| FlagDebugTrackReceive | Debugger aid. |
| FlagDisablePayloadChecksums | Advanced IC/XDC/RDMA trade-off; do not use casually. |

The upper 12 flag bits encode the Interconnect channel via
IEventHandle::MakeFlags. FlagExtendedFormat is runtime serialization metadata,
not an application flag. FlagSystemMessage is also not an application flag:
Receive consumes any event with that bit before StateFunc and before
ActorWaitForEvent. The library Send sites pass TEvResumeRunnable::EventFlags or
TEvCheckActorLiveness::RequestFlags; the handle constructor does not infer the
bit from the C++ event type. If you set it on your own event, the payload is
ignored. In the current tree FlagFailFastWhenDisconnected and FlagUseSubChannel
do not provide a usable application contract; do not design around them.

FlagForwardOnNondelivery takes precedence over TrackDelivery: it transfers the
original payload to the configured fallback instead of creating
TEvUndelivered. A generated TEvUndelivered carries the original type and cookie,
and its envelope Sender is the failed recipient. That notification may itself
be undeliverable.

## Sending, forwarding, and scheduling

Inside an activation:

~~~cpp
Send(recipient, new TEvRequest("x"), flags, cookie);
SelfId().Send(recipient, new TEvRequest("x"), flags, cookie);
TActivationContext::Send(
    new IEventHandle(recipient, SelfId(), new TEvRequest("x"),
                     flags, cookie));
~~~

Outside actor execution:

~~~cpp
actorSystem->Send(
    new IEventHandle(recipient, sender, new TEvRequest("x"),
                     flags, cookie));
~~~

Do not use TLS-bound Send APIs from arbitrary threads. If a non-actor caller
needs a result, ActorSystem::Ask creates a temporary waiter actor and returns a
future. Use a finite timeout: the default may be infinite. Ask consumes the
first event it receives, whatever it is: a matching (or unconstrained) type
resolves the future, any other type fails it with an exception. It does not
authenticate Sender or wait past a stray event, so use a dedicated address or
additional protocol correlation where that distinction matters.

The shorter ActorSystem::Send(recipient, event) overload uses a synthetic
actorsystem service id as Sender. It is not a reply endpoint.

Forward transfers the payload into a newly constructed handle addressed to the
new recipient. It retains basic sender/flags/cookie data, but not every
origin/session/rewrite/nondelivery field. Constructing a new event is clearer
when the protocol needs an explicit envelope contract.

### Common, Lazy, and Tail sending

Common activates an idle target mailbox normally. Lazy and Tail are local
scheduling optimizations:

- Lazy may let the current worker capture an idle target mailbox and postpone
  its activation until the current mailbox finishes. This can improve locality
  but can add latency behind a long current activation.
- Tail ends the current mailbox's run after this event and transfers remaining
  quantum to the captured target, when the executor can do so.

They degrade to ordinary sending where the optimization is unavailable, such
as a foreign thread or unsuitable pool relationship. They do not change
delivery, ownership, or ordering semantics; use them only after measurement.

### Timers

~~~cpp
Schedule(TDuration::Seconds(1), new TEvWakeup);
Schedule(deadlineMonotonic, new TEvWakeup);
~~~

The IActor convenience overload constructs a self-addressed envelope whose
Sender is empty and whose event Cookie is zero. If a logical timer identity is
needed, carry it in the payload or build an IEventHandle explicitly.

ISchedulerCookie is scheduler cancellation arbitration, not IEventHandle::Cookie.
Use a two-way cookie holder immediately when scheduling. Detaching it may
prevent dispatch only if it wins the race with the scheduler; after release to
the mailbox the event may still arrive. Timers are not automatically removed
when an actor dies, and their payload does not keep the actor alive. Make timer
handlers idempotent and validate a generation or monotonic deadline. Prefer
TMonotonic/TDuration for elapsed-time logic; wall clock may jump.

### Actor liveness probe

SendActorLivenessCheck is a local routing probe. A live local ActorId yields
TEvActorAlive and an unknown local ActorId yields TEvActorDead. A remote target
yields TEvActorLivenessUnsure; it is not a distributed health check and says
nothing about application progress.

## Death and cleanup

TEvPoison is an ordinary user event (TEventSimpleNonLocal), including across
nodes. The runtime does not kill the actor for you; the recipient must handle
it and call PassAway. Poison already queued does not discard later Sends; see
Local delivery.

PassAway is cooperative:

1. it aborts on a second call and marks the actor PassedAway;
2. it requests cancellation of every registered top-level stackless coroutine;
3. if any such task remains, it returns while the actor is still registered;
4. the last task to unregister calls FinishPassAway;
5. FinishPassAway detaches the ActorId, and the executor deletes the C++ actor
   after the current Receive has finished.

Nested async<T> frames belong to the top-level task that awaits them; they are
not separately registered actor tasks.

The deferred interval matters. Normal StateFunc handlers can still run. A new
top-level coroutine can run its synchronous prefix but starts already
cancelled, and should unwind at its first cancellation-aware suspension. A
later TEvPoison that still reaches StateFunc and calls PassAway again aborts. If an actor may have
live tasks, switch to a state that drops or deliberately handles late events
before calling PassAway, as in the minimal example. A coroutine initiating
death should do:

~~~cpp
Become(&TThis::StateDying);
PassAway();
co_return;
~~~

A task parked on an awaiter that does not support cancellation can keep the
actor registered indefinitely. Actor-system/mailbox teardown is the emergency
path: DestroyActorTasks force-destroys remaining coroutine frames and then the
actor. It does not run user code after an await, but C++ destruction of live
frame parameters and locals still occurs, so RAII destructors run.

PassAway only requests cancellation; it does not jump out of the calling
handler. A normal completion already in flight may win the race, so make late
post-await work harmless. TActorSystem::Stop is hard teardown: it does not
gracefully Poison every actor. Children and scheduled events also do not
automatically die when a parent calls PassAway.

PassAwayGuard is a move-only RAII guard whose destructor calls PassAway. It is
useful only when that guard is the single owner of the actor's death transition;
combining it with another Poison/death path risks the forbidden second call.

Mailbox aliases created with IActor::RegisterAlias are removed when the actor
detaches. TActorSystem service-map entries are separate and are not removed
with the actor; clear or replace them explicitly.

## ActorId and services

TActorId is a 16-byte value containing a local id, a hint, and encoded node/pool
bits. A normal id is assigned at registration and is valid only for that actor
incarnation. NodeId identifies the owning node; node zero in a service id means
the local node.

ToString and Out are for debugging. They omit pool bits, so parsing the text
cannot generally reconstruct the original id. Do not store that text as an
address or durable database key.

A service id stores up to 12 bytes of name and is mapped locally:

~~~cpp
const TActorId serviceId = TActorId(nodeId, "myservice");
actorSystem->RegisterLocalService(serviceId, actorId);
actorSystem->RegisterLocalService(serviceId, TActorId()); // clear
~~~

Routing performs one service-to-actor lookup. Do not rely on chains of service
aliases. A service name is stable only because application code maintains the
mapping; failover must install the new mapping.

## Mailboxes, pools, and executor behavior

Mailbox serialization is the guarantee; a specific queue algorithm is not.
The current built-in runtime uses a lock-free intrusive mailbox. A mailbox is
assigned to a logical executor pool at registration.

The executor processes events until a configured event/time quantum is reached,
the mailbox empties, Tail transfers execution, or a shared-pool worker is
softly preempted. It checks these limits between events, never inside a
handler. Defaults and YDB production configuration differ, so do not make
correctness depend on a quoted duration or event count.

Logical pools express workload scheduling and accounting. Some configurations
use shared/united executor workers that can lease or switch between pools, so
“pool A owns a disjoint set of OS threads” is not a universal invariant. Do not
assume names such as User/System imply a fixed handler-time budget.

In a YDB node, TAppData still publishes the process's workload pool ids
(SystemPoolId, UserPoolId, IOPoolId, BatchPoolId). Pass those into Register;
do not hard-code numeric pool ids.

Blocking work must leave System/User handlers:

- InvokeIoCallback(callback, AppData()->IOPoolId, activity) posts TEvInvokeQuery
  to the IoDispatcher local service (MakeIoDispatcherActorId). Dedicated OS
  threads ("kikimr IO") run the callback. That callback is not an actor turn:
  marshal completion with TActorSystem::Send. If the dispatcher is not
  registered, Send fails and the helper falls back to an ExecuteLater actor on
  the poolId you passed, which *does* occupy an executor worker.
- CreateInvokeActor(callback, complete, activity) plus Register on IOPoolId
  runs callback in a short-lived actor on the IO executor pool, then sends
  TEvInvokeResult to the parent. Handle that event with Process() or the
  templated GetResult<TCallback>(). This still monopolizes an IO-pool worker
  for the duration of callback.
- Registering a long-lived actor on IOPoolId is for actors whose handlers
  themselves block. Other mailboxes on that pool still starve while it runs.

Same-mailbox actors always serialize with each other even if the pool has many
workers. Different mailboxes may execute concurrently. A blocked worker reduces
pool capacity; enough blocked workers stall all mailboxes that depend on that
capacity.

## Local delivery

For an ordinary local Send:

- if the ActorId is attached, the handle is enqueued;
- if it is unknown or already detached, the event is destroyed;
- with FlagTrackDelivery, supported local nondelivery produces
  TEvUndelivered with ReasonActorUnknown.

Once Receive has accepted the event, TrackDelivery has served its purpose. It
does not report that the handler threw, ignored the event, died before replying,
or failed application work.

Poison is an ordinary event. Events already ahead of it run first. Events
behind it are not magically erased: after immediate detachment they are dropped
as unknown, but if PassAway is waiting for coroutine tasks they may reach the
dying actor's StateFunc. A Send that races after Poison is already queued still
succeeds; the later event is simply FIFO-behind Poison.

## Interconnect

Sending to an ActorId with another NodeId routes through that node's Interconnect
proxy. Only serializable events can cross this boundary. Traffic is split into
channels; there is no application-level total order across channels.

The safe failure contract is:

- a session incarnation may connect and later terminate;
- some messages may have been delivered while other in-flight outcomes are
  ambiguous;
- classic sessions may continue across some TCP reconnects, while IC v2
  intentionally has no such continuation;
- a replacement session is a new ordering and subscription boundary.

Therefore application protocols still need reply timeouts, duplicate tolerance
or idempotency, and recovery from a session change.

### TrackDelivery over Interconnect

On classic-session failure:

- a tracked event still in the channel's unsent Queue gets a definite
  TEvUndelivered;
- a serialized/not-yet-confirmed event gets TEvUndelivered only when
  FlagGenerateUnsureUndelivered was set in addition to FlagTrackDelivery, and
  that notification has Unsure=true because the peer may have received it;
- without the unsure flag, ambiguous events may produce no notification;
- IC v2 is stricter still: tracked events already handed to its engine can be
  dropped at termination with no notification at all.

These session-failure notifications are generated locally on the sending node.
Only the remote ReasonActorUnknown bounce-back crosses the network and can
itself be lost; Unsure notifications are local-only and not serializable.
This is useful diagnostics and fast failure signalling, not an exactly-once
protocol.

### Session subscription

FlagSubscribeOnSession on a remote event subscribes its Sender to that session
incarnation. TEvNodeConnected identifies the established session;
TEvNodeDisconnected means **that session ended**, not necessarily that the node
is down. A healthy idle session may close. A later connection is a different
incarnation.

Explicitly send TEvUnsubscribe when the relationship ends. The current runtime
also performs a slow liveness check for dead local subscribers, but relying on
that retains session state unnecessarily. A session keeps one subscription per
sender ActorId; later subscription information for that sender can replace the
earlier entry.

When a request must be answered on the exact session that delivered it, use
ev->InterconnectSession:

~~~cpp
auto reply = MakeHolder<IEventHandle>(
    ev->Sender, SelfId(), new TEvResponse("ok"), 0, ev->Cookie);
if (ev->InterconnectSession) {
    reply->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
}
TActivationContext::Send(reply.Release());
~~~

That prevents a response from silently moving onto a replacement session. It
does not turn the exchange into durable delivery.

## Data lifetime and large buffers

TRope and shared-buffer event APIs allow cheap slices and ownership transfer.
Use them when the event type supports them; avoid flattening merely to send.
Conversely, retaining an event handle or rope across a long wait retains all of
its backing chunks. Extract small fields and release large payloads early.

Never retain a raw pointer returned by ev->Get after the handle is destroyed,
forwarded, released, or lazily converted from its serialized representation.

## Stackless coroutine summary

The stackless actor library has two roles:

- a void IActor member with co_await is a top-level handler task registered
  with the actor for cancellation and lifetime;
- async<T> is a lazy, owned child frame and must be awaited by value.

Mailbox execution is still serialized, but every co_await is an interleaving
point at which other events and top-level tasks may mutate actor state. Do not
hold locks, TActorContext, event-owned pointers, or invalidatable references
across it. Cancellation is cooperative and normally destroys the frame rather
than returning from the cancelled await, so cleanup belongs in RAII.

The legacy stackful TActorCoro API is a separate mechanism and is not described
by these rules. See [Stackless actor coroutines](#stackless-actor-coroutines)
for frame creation/destruction, cancellation, timeouts, events, task groups,
thread bridging, and examples.

## Non-actor threads

A foreign thread may use a stable TActorSystem pointer to Send or Ask. It must
not call IActor::Send, SelfId().Send, TActivationContext accessors, actor-aware
awaiters, or manipulate actor objects directly. Define who owns the
TActorSystem pointer and ensure no producer uses it after system shutdown.

For asynchronous external APIs, marshal completion back as an event or use the
documented thread-safe stackless-await bridge. Never resume an actor coroutine
directly on the external thread.

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

## Source map

Use the source, not this guide, when changing runtime internals:

| Topic | Primary files |
|---|---|
| Receive, PassAway, actor tasks | library/actors/core/actor.cpp, actor.h |
| Bootstrap | library/actors/core/actor_bootstrapped.h |
| Dispatch macros | library/actors/core/hfunc.h |
| Event envelope and flags | library/actors/core/event.h |
| Event types and nondelivery | library/actors/core/events.h, events_undelivered.cpp |
| Protobuf events and AddPayload | library/actors/core/event_pb.h |
| Registration and local routing | library/actors/core/executor_pool_base.cpp, actorsystem.cpp |
| Mailbox and executor loop | library/actors/core/mailbox_lockfree.*, executor_thread.cpp |
| Pool sharing | library/actors/core/executor_pool_shared.cpp |
| Blocking IO offload | library/actors/core/io_dispatcher.h, invoke.h |
| YDB pool ids | ydb/core/base/appdata_fwd.h (TAppData) |
| Scheduler cookies | library/actors/core/scheduler_cookie.h, scheduler_* |
| ActorId and services | library/actors/core/actorid.*, actorsystem.* |
| Interconnect session/channel | library/actors/interconnect/interconnect_tcp_session*.cpp, interconnect_channel.cpp |
| Stackless coroutines | library/actors/async/ |

The compact rule is: **an actor owns state, a mailbox owns serialization, a
turn owns its context and event, Send transfers ownership but not certainty,
and PassAway owns the one-way transition to death.**

## Stackless actor coroutines

This section describes the stackless NActors async library in the current YDB
tree. It is both a usage guide and a compact correctness contract. The legacy
stackful TActorCoro API is a different mechanism and is intentionally out of
scope.

Primary headers:

- library/actors/async/async.h
- library/actors/async/wait_for_event.h
- library/actors/async/continuation.h
- library/actors/async/event.h
- library/actors/async/sleep.h and yield.h
- library/actors/async/timeout.h
- library/actors/async/cancellation.h
- library/actors/async/task_group.h
- library/actors/async/low_priority.h

Include async.h in the translation unit that defines a coroutine actor method.
It provides the coroutine_traits specialization that turns an actor member
returning void into a top-level actor task.

---

### The compact contract

1. **Coroutines do not add threads.** Actor code remains mailbox-serialized.
   Several frames may be suspended, but only one actor turn or continuation
   executes at a time.

2. **There are two different frame owners.** A void actor coroutine is a
   top-level task registered with the actor. An async<T> is a lazy child frame
   owned by the expression that awaits it.

3. **Top-level parameters are values.** Every explicit parameter of a void
   actor coroutine must be a non-reference type. In particular, an async hFunc
   handler takes TEv::TPtr ev, not TEv::TPtr&.

4. **A top-level call is immediate and unstructured.** It runs synchronously
   until its first real suspension and returns no handle. Calling another void
   coroutine does not create a child relationship; use async<T> and co_await
   when sequencing and ownership matter.

5. **An async<T> call is lazy.** The call creates a suspended frame; its body
   starts only when directly awaited. Dropping it destroys the unstarted frame.

6. **Every possible suspension is an interleaving point.** Another event or
   top-level task may change actor state before this frame resumes. Mailbox
   serialization prevents data races, not stale references or invalid logic.

7. **Cancellation is cooperative control flow, not an exception.** Code after
   the cancelled suspension is normally skipped; live frame objects are
   destroyed, so RAII runs. catch (...) does not catch cancellation.

8. **PassAway requests cancellation and may return.** The actor remains
   registered until every top-level task finishes cancellation. A
   non-cancellable wait may postpone death indefinitely.

9. **Actor-aware primitives are actor-local.** Continuation Resume/Throw/drop,
   TAsyncEvent Notify/destruction, cancellation-scope Cancel, and runnable
   scheduling require the correct actor activation. A foreign thread sends an
   event or completes a generic bridged awaitable.

10. **Timeout is not a hard deadline.** It requests child cancellation and
    waits for confirmation. Normal completion may still win after the deadline.

11. **RAII cleanup must survive hard teardown.** Forced mailbox cleanup
    destroys frames without resuming their bodies and may not provide usable
    actor TLS. Destructors should release memory and links without depending on
    Send or TActorContext.

12. **Await temporary objects directly.** The library's non-movable,
    nodiscard awaitables are designed as direct operands of co_await. Storing a
    wrapper or splitting a coroutine-lambda invocation from its await can leave
    a lazy frame borrowing a dead closure.

### Two frame kinds

| Property | Top-level actor coroutine | Nested async<T> |
|---|---|---|
| Signature | Non-static IActor member returning void | Function/member/lambda returning async<T> |
| Start | Immediately, initial_suspend is suspend_never | Lazily, initial_suspend is suspend_always |
| Return object | None | A non-copyable, normally non-movable async<T> |
| Owner | Actor's TActorTask list | Direct co_await expression / decorator |
| Actor lifetime | PassAway waits for it | Lives under the top-level task that awaits it |
| Result | None | T or void; exception rethrown at co_await |
| Cancellation | Root cancellation source | Inherited from the awaiting parent |

#### Top-level frame lifecycle

Calling a void actor coroutine logically performs:

1. allocate the coroutine frame and copy/move all explicit value parameters
   into it;
2. construct TActorAsyncHandlerPromise, which registers itself as a TActorTask
   on the actor;
3. because initial_suspend never suspends, execute the body immediately;
4. at a real co_await, leave the frame parked and return void to the caller;
5. on normal completion, destroy body locals, reach final suspend, destroy the
   frame, and unregister the task in the promise destructor;
6. on confirmed cancellation, destroy the parked frame and therefore all live
   locals, awaiters, parameters, and the promise;
7. on hard mailbox cleanup, DestroyActorTasks calls coroutine_handle::destroy
   directly, with the same C++ object destruction but no further body code.

If the body never actually suspends, registration, execution, finalization, and
frame destruction all finish before the call returns.

The caller receives no task handle. This is appropriate for an event entry
point or an intentionally independent actor background loop. Calling another
void coroutine from a handler does not wait for it; it starts a sibling
top-level task:

~~~cpp
void Handle(TEvRequest::TPtr ev) {
    ChildRoot();              // sibling TActorTask; Handle does not wait
    co_await SomethingElse();
}

void ChildRoot() {            // also a top-level void coroutine
    co_await Work();
}
~~~

Prefer a nested async<T> that Handle co_awaits:

~~~cpp
void Handle(TEvRequest::TPtr ev) {
    co_await Child();
}

async<void> Child() {
    co_await Work();
}
~~~

#### Nested frame lifecycle

Calling an async<T> function:

1. creates its frame and stores its parameter copies/references;
2. stops at initial_suspend before executing the body;
3. returns an async<T> object that owns the frame handle.

Then one of two things happens:

- if the async object is discarded, its destructor destroys the frame; body
  locals were never constructed and the body never runs;
- if directly awaited, the await expression installs the actor, cancellation
  source, and parent continuation, then transfers execution into the child.

On child completion, final suspend transfers back to the parent. The promise
retains the value or exception until await_resume extracts it. At the end of
that co_await full-expression the owning async object destroys the child frame.
An exception stored by a nested child is rethrown in the parent at co_await.

async<T> is deliberately not a general future or detachable task. Do not use
UnsafeMove in ordinary application code; it exists for library combinators.

### Parameters, captures, and frame-owned data

The top-level specialization is selected only for:

~~~cpp
template<IsActorSubClassType T, IsNonReferenceType... Args>
struct std::coroutine_traits<void, T&, Args...>;
~~~

Therefore all explicit arguments must be values:

~~~cpp
void Handle(TEvRequest::TPtr ev) { // correct: event ownership enters frame
    co_await Process();
}

void Handle(TEvRequest::TPtr& ev) { // not a stackless actor coroutine promise
    co_await Process();             // compile-time failure
}
~~~

hFunc passes its TAutoPtr lvalue to the by-value handler, transferring event
ownership. The hidden this reference is expected and is not one of Args.
Coroutine Bootstrap overloads must likewise take parent/context by value when
present. Install Become before Bootstrap's first suspension.

Nested async<T> functions may have reference parameters, but those are ordinary
borrows:

~~~cpp
async<void> Use(const TString& value); // caller must keep value alive
~~~

A by-value top-level event handle remains memory-safe across suspension because
the frame owns it. It may nevertheless retain a large payload for a long time.
Extract the small fields that are needed and ev.Reset() before a long wait.

Coroutine lambdas have an extra lifetime trap: their frames generally borrow
the lambda closure through its this pointer. These are safe:

~~~cpp
auto operation = [value]() -> async<void> {
    co_await Use(value);
};
co_await operation(); // named closure outlives the await

co_await WithTimeout(TDuration::Seconds(1), [value]() -> async<void> {
    co_await Use(value);
}); // closure temporary lives through this co_await full-expression
~~~

Do not create/store the returned lazy async or a decorator in one full-expression
and await it in another. TTaskGroup::Add is different: it stores a decayed copy
of the callback and arguments until the child finishes.

In general, avoid coroutine lambdas as much as possible.

---

### Minimal request/reply actor

The event definitions are omitted; the important details are value parameters,
unique cookies, a nested helper, timeout handling, stale-reply dispatch, and a
dying state.

~~~cpp
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

using namespace NActors;

class TClient final : public TActorBootstrapped<TClient> {
public:
    explicit TClient(TActorId backend)
        : Backend(backend)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    void Handle(TEvStart::TPtr ev) {
        const TActorId replyTo = ev->Sender;
        const ui64 replyCookie = ev->Cookie;
        TString payload = ev->Get()->Payload;
        ev.Reset(); // do not retain the incoming envelope while waiting

        auto value = co_await WithTimeout(
            TDuration::Seconds(5),
            RoundTrip(std::move(payload)));

        if (value) {
            Send(replyTo, new TEvDone(*value), 0, replyCookie);
        } else {
            Send(replyTo, new TEvFailed("timeout"), 0, replyCookie);
        }
    }

    async<int> RoundTrip(TString payload) {
        const ui64 cookie = ++NextCookie;
        Send(Backend, new TEvBackendRequest(std::move(payload)), 0, cookie);
        auto reply = co_await ActorWaitForEvent<TEvBackendReply>(cookie);
        co_return reply->Get()->Value;
    }

    void BeginShutdown() {
        Become(&TThis::StateDying);
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvStart, Handle);
        IgnoreFunc(TEvBackendReply); // late reply after timeout/cancellation
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev);
    }

private:
    const TActorId Backend;
    ui64 NextCookie = 0;
};
~~~

Each TEvStart starts an independent top-level task. Those tasks overlap while
waiting but never execute simultaneously. ActorWaitForEvent cancels promptly,
so this particular timeout can complete cooperatively. A different child
awaiter may not.

ActorWaitForEvent matches only type and cookie, not Sender. The example assumes
only Backend uses these reply cookies. If sender authentication matters, route
replies through StateFunc or a protocol-specific stream that validates it.

### Execution and resumption

A top-level handler begins inside the StateFunc or Bootstrap call and runs
synchronously until it suspends. While it is parked, Receive continues handling
mailbox events:

| Incoming work | What happens |
|---|---|
| Event matching ActorWaitForEvent | The awaiter consumes it and resumes inline inside Receive. |
| Non-matching user event | It reaches the current StateFunc and may start another root task. |
| Scheduled actor runnable | It resumes when the current runnable queue drains. |
| TEvResumeRunnable system event | It resumes during that later mailbox event. |

TActorRunnableQueue is installed for each Receive. Primitives such as
TAsyncContinuation and TAsyncEvent schedule runnables so they do not recursively
resume another frame from Resume/Notify. The queue drains before Receive
finishes. ActorWaitForEvent is deliberately different: a matched event resumes
its waiter inline and never reaches StateFunc.

AsyncSleep, AsyncYield, finite timers, and generic off-thread bridges use a
TEvResumeRunnable mailbox hop. They do not execute actor code on a scheduler or
foreign thread. Library Send sites pass TEvResumeRunnable::EventFlags
(FlagSystemMessage). If that event is destroyed unhandled — actor already gone,
mailbox teardown, or Item nulled to cancel — ~TEvResumeRunnable calls
Item->Run(nullptr). You may set Item to nullptr only on the target actor while
the event is still inflight.

#### Interleaving rules

Treat every co_await that may suspend as:

~~~text
save frame state
allow arbitrary later actor turns
resume under the mailbox
revalidate assumptions
~~~

In particular:

- Become is actor-global; another handler can change the state function.
- A map entry, pointer, iterator, transaction context, or generation may be
  erased or replaced while this frame waits.
- A reference into an incoming event is safe only while the frame still owns
  that event; resetting/forwarding the handle invalidates it.
- A TActorContext is never safe across suspension. It refers to the activation
  and worker that ran the old turn.
- Never keep a mutex or transaction/DB guard across suspension.
- The actor's this pointer remains alive while its top-level task is registered,
  but a raw pointer to another actor has no such protection.

The robust production pattern is to keep a request id/generation in the frame
and re-look up actor-owned state after each suspension. Mailbox serialization
does not make a previously saved pointer logically valid.

---

### Cancellation, PassAway, and death

Cancellation is an alternate coroutine-unwind protocol implemented with
await_cancel and await_cancelled hooks. It is not throw/catch.

When a root task is cancelled:

1. the request is latched in its promise;
2. the currently suspended awaiter is asked to cancel if it supports the
   extension;
3. the awaiter may confirm immediately, perform asynchronous cleanup and
   confirm later, or resume normally because completion won the race;
4. on confirmed unwind the frame is destroyed and live RAII objects run;
5. its promise unregisters the root task from the actor.

Important consequences:

- code after the cancelled co_await usually does not run;
- catch (...) is not a cancellation handler;
- an await_ready operation may complete without observing cancellation;
- an already-cancelled task can execute a synchronous prefix before its first
  cancellation-aware suspension;
- an awaiter with no cancellation support may still suspend and later resume
  normally;
- completion already scheduled before cancellation often wins.

#### PassAway

PassAway marks the actor dying and calls Cancel on every root TActorTask. It
detaches the ActorId only after the last root unregisters. Until then normal
StateFunc events can still arrive, including another Poison. Since PassAway is
one-shot, switch to a dying state before calling it.

PassAway does not jump out of the current coroutine. Use:

~~~cpp
Become(&TThis::StateDying);
PassAway();
co_return;
~~~

Do not perform more shutdown-sensitive work after PassAway. A cancellation
request can lose to a normal resume, so make already-scheduled post-await work
check a dying flag/generation and remain harmless.

A task parked on a generic TFuture or custom awaiter without cancellation may
keep the actor registered indefinitely. TActorSystem shutdown is harder:
mailbox cleanup force-destroys all remaining root frames before deleting the
actor.

### RAII: what runs and when

| Exit path | Body continues? | Live frame destructors run? | Actor TLS reliable? |
|---|---|---|---|
| Normal co_return | Yes, through normal scopes | Yes | Yes during the turn |
| Nested exception | Stack unwinds to a catch/parent | Yes | Yes during the turn |
| Confirmed cancellation | No code after cancelled await | Yes | Normally during actor execution |
| Never-awaited async<T> dropped | Body never starts | Promise/parameters only; no body locals | Not required |
| Forced mailbox teardown | No resumption/body cleanup code | Yes, via handle.destroy | Do not rely on it |

Use ordinary RAII or Y_DEFER for synchronous ownership cleanup:

- erase actor-owned request state;
- unlink registrations;
- release buffers, locks that are not held across awaits, and intrusive refs;
- mark completion and notify owner state where actor context is guaranteed.

Destructors must be safe if only memory ownership remains. During hard teardown
do not unconditionally Send, access TActorContext, or touch executor-owned
counters. Existing production cleanup commonly guards such optional work with
if (TlsActivationContext), while always doing pure state/memory cleanup.

RAII cannot co_await. If cancellation requires an asynchronous unsubscribe or
rollback, structure it as awaited work or use InterceptCancellation. Do not
hide async cleanup in a destructor.

Two primitives have especially relevant destructor behavior:

- destroying a live TAsyncContinuation schedules its waiter with a logic_error;
- destroying a TAsyncEvent with waiters schedules them with false.

Those operations are valid only under the proper actor runnable queue. Ensure
the objects are empty before foreign-thread or non-activation destruction.

#### Exceptions

A nested async<T> stores an exception and rethrows it at the parent's
await_resume, so normal try/catch and RAII apply:

~~~cpp
try {
    co_await Child();
} catch (const TExpectedError& e) {
    // protocol error handling
}
~~~

An exception escaping a top-level void handler is offered to
IActorExceptionHandler. If it is not accepted, the runtime terminates. A
cancellation never enters that catch block.

---

### Choosing an await primitive

| Need | Use |
|---|---|
| One protocol reply identified by type and cookie | ActorWaitForEvent |
| Park until later code on *this* actor calls Resume | WithAsyncContinuation |
| Wake one/all actor-local waiters without storing a signal | TAsyncEvent |
| Wait for time | AsyncSleepFor / AsyncSleepUntil |
| Yield once, injecting your own self-event | AsyncYield / AsyncSleepFor(0) |
| Cooperative child deadline | WithTimeout / WithDeadline |
| Cancel a subset of work | TAsyncCancellationScope |
| Structured fan-out/fan-in | WithTaskGroup |
| Many tasks yield without flooding the mailbox | TAsyncLowPriorityQueue |
| Completion from a foreign thread | Generic awaitable bridge, or ActorSystem::Send plus ActorWaitForEvent |

### ActorWaitForEvent

~~~cpp
const ui64 cookie = ++NextCookie;
Send(peer, new TEvRequest, 0, cookie);
auto ev = co_await ActorWaitForEvent<TEvReply>(cookie);
~~~

Contract:

- matches GetTypeRewrite plus Cookie; it does not match Sender;
- ActorWaitForEvent<IEventHandle> matches any non-system user event with that
  cookie;
- matching occurs before StateFunc and transfers the whole event handle to the
  waiter;
- resumption is inline in the Receive that accepted the event;
- cancellation/destruction unregisters the waiter;
- wrong type/cookie falls through to other waiters and then StateFunc.

Use a cookie unique among every live wait for the same possible reply type.
Send's cookie defaults to 0; do not leave concurrent waits on cookie 0. When
several waiters have the same type and cookie, one event wakes the first
registered match; the intent is ambiguous.

Sending the request immediately before awaiting is safe in an actor turn: the
mailbox cannot process its reply before this code reaches suspension. A reply
that was already processed before the triggering event cannot be recovered.

After cancellation or timeout, a late reply is no longer intercepted and
reaches StateFunc. A STRICT_STFUNC actor must explicitly handle/ignore stale
reply types. Generation checks are still needed because a cookie may eventually
be reused.

### AsyncSleep and AsyncYield

~~~cpp
co_await AsyncYield();
co_await AsyncSleepFor(TDuration::MilliSeconds(10));
co_await AsyncSleepUntil(monotonicDeadline);
~~~

- Sleep always suspends.
- Zero duration sends a self TEvResumeRunnable, so it resumes on a later
  mailbox event; AsyncYield is this zero-duration operation.
- An infinite duration/deadline schedules nothing and waits only for
  cancellation or forced destruction.
- Finite nonzero sleep uses a scheduler event and a refcounted bridge.
- Cancellation disarms the bridge, but cannot remove the scheduler item; a
  cancelled long sleep retains a small allocation until deadline/shutdown.

Prefer TMonotonic for elapsed-time deadlines. Sleep is actor-aware and must be
started from an actor coroutine.

### WithAsyncContinuation

Use this when a coroutine must wait for work that is already driven **on this
actor** by something other than a mailbox event you can steal: a tablet
`Execute`/`Complete`, a waiter list filled by a later `Handle`, a cache that
will call you back when populated. You store a `TAsyncContinuation<T>` and some
later turn of **this** actor calls `Resume`.

Do **not** use it for:

- a peer reply — that is `ActorWaitForEvent` (the wake-up *is* the event);
- several waiters on a flag with no value — that is `TAsyncEvent`;
- completion on another thread — `Send` an event, or `co_await` a generic
  `TFuture`. `Resume` is not thread-safe.

The setup lambda runs synchronously inside `await_suspend`. Its job is only to
take the continuation and arm the existing callback path. Production shape
(DataShard lock-rows / tx-id waiters):

~~~cpp
void Handle(TEvNeedTx::TPtr ev) {
    const TActorId replyTo = ev->Sender;
    const ui64 replyCookie = ev->Cookie;
    ev.Reset();

    ui64 txId = co_await WaitForTxId();
    Send(replyTo, new TEvTxId(txId), 0, replyCookie);
}

async<ui64> WaitForTxId() {
    if (!TxIdCache.empty()) {
        ui64 id = TxIdCache.back();
        TxIdCache.pop_back();
        co_return id;
    }
    co_return co_await WithAsyncContinuation<ui64>(
        [this](TAsyncContinuation<ui64> c) {
            TxIdWaiters.push_back(std::move(c));
            if (TxIdWaiters.size() == 1) {
                Send(TxAllocator, new TEvAllocateTxId);
            }
        });
}

void Handle(TEvAllocateTxIdResult::TPtr ev) {
    ui64 txId = ev->Get()->TxId;
    // Still this actor. Resume parked waiters; check bool after cancel.
    while (!TxIdWaiters.empty()) {
        auto c = std::move(TxIdWaiters.front());
        TxIdWaiters.pop_front();
        if (c) {
            c.Resume(txId);
            return;
        }
    }
    TxIdCache.push_back(txId);
}
~~~

`TTxLockRows` is the same idea without a user event: `Execute` a transaction
that holds `TAsyncContinuation<void>` and `Resume()`s from `Execute` or
`Complete`.

The continuation is move-only and one-shot:

- `Resume(value)` completes normally (`Resume()` when `T` is void);
- `Throw(exception_ptr)` makes `await_resume` throw;
- destroying or assigning over a live continuation completes with
  `logic_error("continuation object was destroyed")`;
- `Resume`/`Throw` on an empty continuation throws `logic_error`.

`Resume` inside the setup lambda completes without suspension. A later
`Resume`/`Throw`/drop schedules a runnable; it does not recursively execute the
waiter before `Resume` returns.

If the waiting coroutine is cancelled, the stored continuation is detached and
becomes false. Check it before later completion. All live continuation
operations, including destruction, require this actor's runnable queue.

### TAsyncEvent

TAsyncEvent is an actor-local, non-sticky notification queue:

~~~cpp
bool notified = co_await Changed.Wait();
Changed.NotifyOne();
Changed.NotifyAll();
~~~

- NotifyOne wakes the oldest queued waiter; NotifyAll wakes all current
  waiters.
- Notify with no waiters is lost. It is not a latch and stores no count.
- Wait returns true for notification.
- Destroying the event detaches current waiters and they return false; this is
  normal completion, not cancellation.
- Cancellation while still queued removes the waiter and unwinds it instead of
  returning false.
- Once a notification/detach runnable is scheduled, a later cancellation
  normally loses and Wait resumes normally.

Notify and destruction with waiters require the owning actor activation.
Do not share one TAsyncEvent across unrelated actors/mailboxes or manipulate it
from a foreign thread.

Wait(callback) first registers the awaiter and then calls callback synchronously.
It is useful for counters/start logic without a lost wakeup between registration
and callback.

### WithTimeout and WithDeadline

~~~cpp
bool completed = co_await WithTimeout(
    TDuration::Seconds(1),
    DoVoidWork());

std::optional<int> value = co_await WithDeadline(
    deadline,
    DoValueWork());
~~~

For an async<void> child the result is bool; for async<T> it is optional<T>.
true/a value means normal completion. false/nullopt means timeout-triggered
cancellation was confirmed.

The deadline sequence is:

1. start a timer and the lazy child;
2. if the timer wins, request child cancellation;
3. wait for the child to confirm unwind;
4. only then return false/nullopt.

It is therefore not a hard wall-clock bound:

- a non-cancellable child awaiter can postpone return forever;
- the child may resume normally after the timeout request, in which case its
  successful result wins;
- immediately ready work may beat a zero timeout;
- caller/PassAway cancellation propagates as cancellation of the caller; it
  does not return false;
- an infinite timeout bypasses decoration.

The scheduled timer cannot be deterministically removed, so a disarmed long
timeout keeps a small bridge until deadline/shutdown. Use a protocol-level
generation/idempotency rule for work whose external effect can outlive the
local timeout.

Keep WithTimeout/WithDeadline and their callback/async argument as the direct
operand of the same co_await.

### Cancellation scopes

TAsyncCancellationScope cancels selected child operations independently from
the actor's root cancellation.

**Its destructor does not cancel work.** Call Cancel explicitly. Cancel is
sticky/idempotent at scope level; a sink attached after cancellation is
cancelled immediately. Cancel and live-sink manipulation are actor-local.

#### Wrap

~~~cpp
TAsyncCancellationScope scope;

bool completed = co_await scope.Wrap([&]() -> async<void> {
    co_await WaitForWork();
});

scope.Cancel(); // normally called by another handler while Wrap is waiting
~~~

Wrap<void> returns bool and Wrap<T> returns optional<T>. false/nullopt means the
scope's cancellation was confirmed. The request can lose to normal completion,
which then returns success. Cancellation of the caller/root normally unwinds
past Wrap instead of returning false.

An already-cancelled scope still starts the lazy body. Its synchronous prefix
can run, then it should unwind at its first cancellation-aware suspension.

#### Attaching the whole root handler

Inside a top-level void handler only:

~~~cpp
Request.Scope =
    co_await TAsyncCancellationScope::WithCurrentHandler();
~~~

The returned scope contains that root handler as a sink. A later
Request.Scope.Cancel() requests cancellation of the whole handler. Use it
directly in the root; it relies on the root promise's special await_transform.

This is useful for actor-owned keyed requests: store a scope beside request
state, cancel it on duplicate/explicit cancel, and let an RAII guard erase the
state and notify completion. Since cancellation is deferred, a replacement may
need to await an explicit Finished TAsyncEvent before reusing the key.

#### WrapShielded and InterceptCancellation

WrapShielded prevents caller cancellation from entering the wrapped child until
the child completes. It does not clear the caller's latched cancellation, and
explicit scope.Cancel still cancels the child. Shielding can delay PassAway
indefinitely; reserve it for cleanup that truly must finish.

InterceptCancellation(body, onCancel) is the advanced hook for cleanup that
must run when caller cancellation arrives. onCancel may return:

- void or true to propagate cancellation;
- false to decline propagation;
- async<void> or async<bool> for awaited cancellation handling.

An exception from onCancel is rethrown after inner cleanup. Prefer simple RAII
unless asynchronous/vetoing cancellation is genuinely required.

### Task groups

WithTaskGroup provides structured cooperative fan-out. Children are async<T>
frames on the same actor, not threads.

~~~cpp
int total = co_await WithTaskGroup<int>(
    [&](TTaskGroup<int>& group) -> async<int> {
        group.Add([this]() -> async<int> {
            co_return co_await Fetch(1);
        });
        group.Add([this]() -> async<int> {
            co_return co_await Fetch(2);
        });

        int sum = 0;
        while (group.Running() || group.Ready()) {
            sum += co_await group.Next();
        }
        co_return sum;
    });
~~~

Semantics and corners:

- Add returns a monotonically assigned child index.
- Add stores decayed callback/argument copies and schedules startup on the
  runnable queue; the callback does not run recursively.
- If the group body returns before a scheduled child starts, that callback may
  never run.
- Results queue in completion order.
- Next consumes a value and rethrows a child exception.
- NextResult returns TTaskGroupResult<T> with GetIndex plus value/exception;
  use HasValue/HasException and ExtractValue/ExtractException.
- WhenReady waits without consuming.
- Only one group waiter is allowed. Awaiting with no ready and no unfinished
  task throws.
- operator bool and Running count unfinished tasks only. Ready-but-unconsumed
  results are not counted, so robust full draining tests Running() || Ready().

When the group body returns, throws, or confirms cancellation, the wrapper
cancels every unfinished child and does not return to the outer caller until
their cooperative unwind finishes. There is no detached child work.

RAII corner: automatic locals of the group-body coroutine are destroyed when
that body exits, before final-suspend handling begins to cancel outstanding
children. A child must not borrow those locals if the body can return early.
Drain such children first or capture owning state whose lifetime spans the
outer WithTaskGroup await.

On actor cancellation the group body must first unwind/return before the group
starts cancelling children. A non-cancellable wait in either the body or a
child can delay group exit and actor death.

### TAsyncLowPriorityQueue

Use this when coroutines must **give the mailbox a turn** (so client events
keep running) without each injecting its own `TEvResumeRunnable`.

`AsyncYield` / `AsyncSleepFor(0)` sends one self-event per waiter. Fifty
handlers yielding at once enqueue fifty events; one activation may drain a
burst of them and starve incoming work until the quantum ends.

`TAsyncLowPriorityQueue` is a shared actor member: at most one resume event is
in flight. `co_await Idle.Next()` parks until a **later mailbox cycle**, then
resumes **one** waiter. While handling that resume event the queue immediately
sends the next one, which is processed on a later cycle behind user events
already queued. Waiters that join after the current event was already sent go
into the next generation and cannot hitchhike on that cycle (including if the
original waiter cancels).

Typical use: slice CPU-heavy work so each item yields, and share one queue
across concurrent handlers:

~~~cpp
TAsyncLowPriorityQueue Idle; // actor member, one per actor

void Handle(TEvCompact::TPtr ev) {
    TVector<TKey> keys = std::move(ev->Get()->Keys);
    ev.Reset();
    co_await CompactKeys(std::move(keys));
}

async<void> CompactKeys(TVector<TKey> keys) {
    for (const TKey& key : keys) {
        CompactOne(key);     // keep this bounded
        co_await Idle.Next(); // other Handles run before the next key
    }
}

void Handle(TEvClient::TPtr ev) {
    // still served between CompactOne slices
    Send(ev->Sender, new TEvClientAck, 0, ev->Cookie);
}
~~~

A single `co_await AsyncYield()` is enough when only one frame needs to breathe
and you do not care about a shared policy.

All waiters must belong to the same actor. The queue must outlive them;
destroying it with waiters aborts. Cancellation unlinks a waiter and may disarm
or reuse the pending self-event.

---

### Foreign-thread completion

There are two supported models. Pick one completion path.

#### Send an actor event

This is the clearest choice when an I/O/completion thread already has
TActorSystem* and the target ActorId:

~~~cpp
// Actor:
const ui64 cookie = ++NextCookie;
StartIo(SelfId(), cookie);
auto done = co_await ActorWaitForEvent<TEvIoDone>(cookie);

// Foreign thread:
actorSystem->Send(
    new IEventHandle(actorId, senderId, new TEvIoDone(status), 0, cookie));
~~~

Use TActorSystem::Send, never IActor::Send, SelfId().Send, or
TActivationContext::Send from the foreign thread. The latter APIs need actor
TLS. The completion event is an ordinary mailbox event and may arrive late
after cancellation, so StateFunc must tolerate it.

For fan-out, let the foreign thread Send one event; inside that actor handler,
update state and call continuation.Resume or TAsyncEvent::NotifyAll.

#### Await a generic C++ awaitable

An awaiter without IsActorAwareAwaiter is wrapped in a thread-safe proxy. At
suspension the library creates a bridge containing the actor, ActorId,
mailbox, and ActorSystem*. The external operation receives a bridge coroutine
handle, not the actor frame.

When that handle is resumed:

- on the same mailbox activation, the bridge schedules a runnable;
- otherwise, including a foreign thread, it uses ActorSystem::Send to enqueue
  TEvResumeRunnable;
- actor code resumes only when the target mailbox processes that work.

NThreading::TFuture is a generic awaitable (include
library/cpp/threading/future/core/coroutine_traits.h; future.h alone does not
provide operator co_await). Its completion may run off-thread, but it does not
implement actor cancellation. Thus PassAway normally waits for the future to
complete; a future that never completes can hold actor death forever.

Generic awaiter obligations:

- complete exactly once;
- for one bridge, resume or destroy it exactly once; when a cancellable
  protocol supplies normal/cancel bridge alternatives, complete exactly one
  alternative and never touch both;
- never also Send a second wakeup for the same wait;
- arrange eventual completion/cancellation so bridges are not leaked;
- ensure the captured TActorSystem still exists when a late off-thread
  completion tries to enqueue its resume.

After forced actor cleanup, a late TEvResumeRunnable is usually destroyed
unhandled: ~TEvResumeRunnable calls Item->Run(nullptr), which must not resume
the destroyed actor frame. The poster still needs a live TActorSystem at the
moment it enqueues. Coordinate external source shutdown before destroying the
actor system.

#### What is actor-local

These must not be completed or manipulated from a foreign thread:

- TAsyncContinuation Resume, Throw, assignment, or destruction while live;
- TAsyncEvent Notify/destruction with waiters;
- TAsyncCancellationScope Cancel with live sinks;
- TActorRunnableQueue::Schedule;
- actor-aware awaiter continuation handles;
- IActor::Send, SelfId().Send, and TActivationContext APIs.

Mark a custom awaiter IsActorAwareAwaiter only when all resume, cancel, and
destruction interactions are serialized on the correct actor/mailbox. Marking
an off-thread awaiter actor-aware bypasses the bridge and can run actor code on
the wrong thread.

---

### Writing a custom awaiter

Prefer the library primitives. If a custom awaiter is necessary, first decide
which contract it implements.

#### Actor-aware awaiter

Declare IsActorAwareAwaiter = true only if wakeup is actor-local. It may use
TActorRunnableQueue::Schedule during Receive to avoid recursive resumption.
ActorWaitForEvent is the exceptional inline-resume pattern.

For cooperative cancellation an awaiter may implement:

- await_cancel(cancellationHandle), called while it is actively suspended;
- await_cancelled(cancellationHandle), optionally called when cancellation was
  already latched before suspension.

await_cancel may return void/false for pending cancellation, true for immediate
unwind, or a coroutine handle for cancellation work. Exactly one path must
ultimately win: normal continuation or cancellation confirmation. Never resume
both.

If cancellation was already latched, await_ready still wins when true. For a
non-ready cancellable awaiter, await_cancelled is the hook for that situation;
when await_cancel exists but await_cancelled does not, the library skips normal
await_suspend and unwinds immediately.
An awaiter with neither cancellation hook may still suspend normally.

Any external pointer to the continuation must be retracted in the awaiter's
destructor, because hard teardown can destroy the frame without normal
completion. Do not destroy the actor-aware cancellation-confirmation handle;
resume it according to the protocol.

#### Generic awaiter

Omit IsActorAwareAwaiter. await_suspend receives a bridge handle safe to
complete from another thread. That external operation owns the exactly-once
resume-or-destroy obligation. The awaiter should still expose a real
cancellation mechanism when possible; otherwise PassAway remains parked until
normal completion.

Test at least:

- ready without suspension;
- normal deferred completion;
- cancellation before and during suspension;
- normal completion racing cancellation;
- exception from await_ready/await_suspend/await_resume;
- actor PassAway while parked;
- forced actor-system teardown followed by late external completion;
- no double resume and no retained bridge/frame.

### Common design patterns

#### Many concurrent requests

Each incoming request is its own top-level `Handle` coroutine. Several can be
parked at once; they still run one at a time. That is the normal model: do not
put in-flight requests into a hand-rolled state machine unless you must.

When **another event** must abort one specific request (duplicate id, client
cancel), keep a map from request id to a cancellation scope attached to that
handler:

~~~cpp
struct TInFlight {
    TAsyncCancellationScope Scope;
};
THashMap<ui64, TInFlight> InFlight;

void Handle(TEvRequest::TPtr ev) {
    const ui64 id = ev->Get()->RequestId;
    const TActorId replyTo = ev->Sender;
    const ui64 replyCookie = ev->Cookie;
    TString payload = std::move(ev->Get()->Payload);
    ev.Reset();

    if (auto it = InFlight.find(id); it != InFlight.end()) {
        it->second.Scope.Cancel(); // old handler for this id must unwind
    }

    auto& slot = InFlight[id];
    slot.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();
    Y_DEFER { InFlight.erase(id); };

    Send(Backend, new TEvWork(std::move(payload)), 0, id);
    auto done = co_await ActorWaitForEvent<TEvWorkDone>(id);
    Send(replyTo, new TEvDone(done->Get()->Value), 0, replyCookie);
}

void Handle(TEvCancel::TPtr ev) {
    if (auto it = InFlight.find(ev->Get()->RequestId); it != InFlight.end()) {
        it->second.Scope.Cancel();
    }
}
~~~

Store ids and owning values in the frame, not pointers into `InFlight`. After
every wait, look the slot up again if you still need it. Cancellation is
deferred: a replacement may need an extra `TAsyncEvent` if the key must not be
reused until the old handler's `Y_DEFER` has run.

#### One background loop per key

A **request handler** starts, waits, replies, and finishes. A **background
loop** is a `void` coroutine that is not awaiting a single client request: it
keeps a subscription, a retry/reconnect, or a watch alive until cancelled.

The "key" is whatever you must not duplicate (tablet id, remote ActorId, stream
id). If a loop for that key is already running, do not start another.

`Handle` here is an ordinary (non-coroutine) handler: no `co_await`, so it may
take `TEv::TPtr&`. `WatchLoop` is a **void** actor coroutine (`co_await` in a
`void` member), which makes it a second top-level `TActorTask`. Calling
`WatchLoop(id)` starts that task until its first real suspend, then `Handle`
returns. That is required: if `WatchLoop` returned `async<T>`, `Handle` would
have to `co_await` it and would stay parked for the whole subscription.

Do not write `co_await WatchLoop(id)`.

~~~cpp
struct TWatch {
    TAsyncCancellationScope Scope;
    ui32 Refs = 0;
};
THashMap<ui64, TWatch> Watches; // key = resource id

void Handle(TEvWatch::TPtr& ev) {          // sync handler: TPtr&
    const ui64 id = ev->Get()->Id;
    auto& w = Watches[id];
    ++w.Refs;
    if (w.Refs == 1) {
        WatchLoop(id);                     // starts a sibling root task
    }
}

void Handle(TEvUnwatch::TPtr& ev) {         // sync handler
    auto it = Watches.find(ev->Get()->Id);
    if (it != Watches.end() && --it->second.Refs == 0) {
        it->second.Scope.Cancel();
    }
}

void WatchLoop(ui64 id) {                   // void + co_await => TActorTask
    Y_DEFER { Watches.erase(id); };
    auto it = Watches.find(id);
    Y_ABORT_UNLESS(it != Watches.end());
    it->second.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();

    for (;;) {
        Send(Source, new TEvSubscribe(id), IEventHandle::FlagTrackDelivery, id);
        auto ev = co_await ActorWaitForEvent<IEventHandle>(id);
        ApplyUpdate(id, ev);               // re-find Watches[id] if needed
    }
}
~~~

`Y_DEFER` erases the key when the loop unwinds (unsubscribe, `PassAway`,
undelivered). `Scope.Cancel()` from `TEvUnwatch` is the stop signal. This is
the tablet-resolver pattern: one `TabletStateSubscriptionLoop` per subscribed
actor, started from `SubscribeTabletState` only if the map has no entry yet.

Do not write `co_await WatchLoop(id)`: a `void` loop is not `async<T>`. Do not
hold a pointer/iterator to the map entry across a wait.

#### Fan-out with bounded lifetime

Use WithTaskGroup when a parent must not finish until all children either
finish or unwind. Do not emulate detached children by calling several void
methods; those become unrelated root tasks.

#### External callback API

- If callback completion is guaranteed on this actor, adapt it with
  WithAsyncContinuation.
- If completion may be off-thread, Send an event or expose a generic awaiter.
- If one completion wakes many actor-local waiters, Send once and fan out with
  continuations/TAsyncEvent inside StateFunc.

### Review checklist

- Is this meant to be a root task, or should it return async<T> and be awaited?
- Do all root explicit parameters have non-reference types?
- Does coroutine Bootstrap Become before its first suspension?
- Is every async<T> consumed directly by co_await or by a library combinator
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

### Source and test map

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

- core/tx/datashard/datashard__lock_rows.cpp: keyed request state,
  WithCurrentHandler, Y_DEFER cleanup, duplicate cancellation, and state
  re-lookup after suspension;
- core/tablet/tablet_resolver.cpp: actor-local events, cancellation scopes,
  keyed background loops, and teardown guards;
- core/kqp/node_service/kqp_query_control_plane.cpp: value-parameter async event
  handler and ActorWaitForEvent.

The tests and library source define the contract; production call sites are
examples, not authority.

The one-line model is: **a root void frame belongs to the actor, a lazy
async<T> frame belongs to its direct await, every suspension permits logical
interleaving, cancellation destroys through RAII rather than throwing, and all
actor code resumes under the mailbox.**

# Actor Lifecycle

An actor is a runtime-owned C++ object whose state changes during serialized mailbox turns. This page covers construction, registration, dispatch, services, and execution pools. Start with the invariants in the [Actor System Guide](index.md); see [Failure and Shutdown](failure-and-shutdown.md) for the death transition.

## A minimal actor

```cpp
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
```

TEvRequest and TEvResponse above are local-only. Use a serializable event for a
message that may cross nodes.

## Construction, bootstrap, and state dispatch

`TActor<TDerived>` takes an initial state function and begins waiting for events.
`TActorBootstrapped<TDerived>` installs a bootstrap state; registration injects a
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

```cpp
void State(TAutoPtr<IEventHandle>& ev);
```

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

```cpp
void HandleAsync(TEvRequest::TPtr ev) {
    // ...
    co_await Something();
}

STRICT_STFUNC(StateWork,
    hFunc(TEvRequest, HandleAsync);
)
```

hFunc passes an lvalue TAutoPtr, whose copy-like operation transfers ownership
into the by-value coroutine parameter. A reference parameter does not select
the actor coroutine promise and fails to compile. See
[Stackless actor coroutines](coroutine-actors.md) for the complete
contract.

## Registration and ownership

From an actor turn:

```cpp
TActorId child = Register(new TChild);
TActorId other = Register(
    new TChild,
    TMailboxType::HTSwap,
    AppData()->UserPoolId);
TActorId colocated = RegisterWithSameMailbox(new THelper);
```

From code with a TActorSystem pointer:

```cpp
TActorId id = actorSystem->Register(
    new TExampleActor,
    TMailboxType::HTSwap,
    poolId);
```

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

## ActorId and services

TActorId is a 16-byte value containing a local id, a hint, and encoded node/pool
bits. A normal id is assigned at registration and is valid only for that actor
incarnation. NodeId identifies the owning node; node zero in a service id means
the local node.

ToString and Out are for debugging. They omit pool bits, so parsing the text
cannot generally reconstruct the original id. Do not store that text as an
address or durable database key.

A service id stores up to 12 bytes of name and is mapped locally:

```cpp
const TActorId serviceId = TActorId(nodeId, "myservice");
actorSystem->RegisterLocalService(serviceId, actorId);
actorSystem->RegisterLocalService(serviceId, TActorId()); // clear
```

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
  templated `GetResult<TCallback>`(). This still monopolizes an IO-pool worker
  for the duration of callback.
- Registering a long-lived actor on IOPoolId is for actors whose handlers
  themselves block. Other mailboxes on that pool still starve while it runs.

Same-mailbox actors always serialize with each other even if the pool has many
workers. Different mailboxes may execute concurrently. A blocked worker reduces
pool capacity; enough blocked workers stall all mailboxes that depend on that
capacity.

## Non-actor threads

A foreign thread may use a stable TActorSystem pointer to Send or Ask. It must
not call IActor::Send, SelfId().Send, TActivationContext accessors, actor-aware
awaiters, or manipulate actor objects directly. Define who owns the
TActorSystem pointer and ensure no producer uses it after system shutdown.

For asynchronous external APIs, marshal completion back as an event or use the
documented thread-safe stackless-await bridge. Never resume an actor coroutine
directly on the external thread.

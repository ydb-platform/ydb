# Events and Messaging

An actor message consists of an owned payload and an envelope containing routing and correlation information. This page explains local and remote delivery, forwarding, timers, and payload lifetime. See the [Actor System Guide](index.md) for mailbox invariants and [Failure and Shutdown](failure-and-shutdown.md) for cleanup.

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

- `TEventLocal<T, Type>` is an in-process C++ payload and is not serializable.
- `TEventPB<T, Proto, Type>` is the normal protobuf-backed wire event. Binary
  blobs that should not live in the protobuf go beside it: AddPayload(TRope)
  returns an index; store that index in the proto; the peer reads GetPayload(i)
  / GetPayloadCount(). Do not stuff TRcBuf into protobuf bytes fields when you
  need zero-copy. The wire encoding is an implementation detail (currently
  marker 0x06, payload sizes, blobs, then the proto; 0x07 is a legacy layout
  accepted on parse only).
- `TEventSimpleNonLocal<T, Type>` is serializable but has no derived payload.
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

```cpp
Send(recipient, new TEvRequest("x"), flags, cookie);
SelfId().Send(recipient, new TEvRequest("x"), flags, cookie);
TActivationContext::Send(
    new IEventHandle(recipient, SelfId(), new TEvRequest("x"),
                     flags, cookie));
```

Outside actor execution:

```cpp
actorSystem->Send(
    new IEventHandle(recipient, sender, new TEvRequest("x"),
                     flags, cookie));
```

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

```cpp
Schedule(TDuration::Seconds(1), new TEvWakeup);
Schedule(deadlineMonotonic, new TEvWakeup);
```

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

```cpp
auto reply = MakeHolder<IEventHandle>(
    ev->Sender, SelfId(), new TEvResponse("ok"), 0, ev->Cookie);
if (ev->InterconnectSession) {
    reply->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
}
TActivationContext::Send(reply.Release());
```

That prevents a response from silently moving onto a replacement session. It
does not turn the exchange into durable delivery.

## Data lifetime and large buffers

TRope and shared-buffer event APIs allow cheap slices and ownership transfer.
Use them when the event type supports them; avoid flattening merely to send.
Conversely, retaining an event handle or rope across a long wait retains all of
its backing chunks. Extract small fields and release large payloads early.

Never retain a raw pointer returned by ev->Get after the handle is destroyed,
forwarded, released, or lazily converted from its serialized representation.

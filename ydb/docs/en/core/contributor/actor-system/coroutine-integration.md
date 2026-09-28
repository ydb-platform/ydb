# Coroutine Integration

External callbacks and keyed operations need explicit ownership across actor turns. This page covers thread bridging, custom awaiters, and background work using the contracts in [Coroutine Actors](coroutine-actors.md) and [Coroutine Primitives](coroutine-primitives.md).

## Foreign-thread completion

There are two supported models. Pick one completion path.

### Send an actor event

This is the clearest choice when an I/O/completion thread already has
TActorSystem* and the target ActorId:

```cpp
// Actor:
const ui64 cookie = ++NextCookie;
StartIo(SelfId(), cookie);
auto done = co_await ActorWaitForEvent<TEvIoDone>(cookie);

// Foreign thread:
actorSystem->Send(
    new IEventHandle(actorId, senderId, new TEvIoDone(status), 0, cookie));
```

Use TActorSystem::Send, never IActor::Send, SelfId().Send, or
TActivationContext::Send from the foreign thread. The latter APIs need actor
TLS. The completion event is an ordinary mailbox event and may arrive late
after cancellation, so StateFunc must tolerate it.

For fan-out, let the foreign thread Send one event; inside that actor handler,
update state and call continuation.Resume or TAsyncEvent::NotifyAll.

### Await a generic C++ awaitable

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

### What is actor-local

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

## Writing a custom awaiter

Prefer the library primitives. If a custom awaiter is necessary, first decide
which contract it implements.

### Actor-aware awaiter

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

### Generic awaiter

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

## Common design patterns

### Many concurrent requests

Each incoming request is its own top-level `Handle` coroutine. Several can be
parked at once; they still run one at a time. That is the normal model: do not
put in-flight requests into a hand-rolled state machine unless you must.

When another event must cancel a specific request, keep a map from request id
to a cancellation scope attached to that handler. The following fragment rejects
duplicate live ids with an application-defined `TEvBusy` response. Cancellation
does not release the id until the old frame has actually unwound.

```cpp
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
        Send(replyTo, new TEvBusy, 0, replyCookie);
        co_return;
    }

    auto& slot = InFlight[id];
    slot.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();
    Y_DEFER { InFlight.erase(id); };

    const ui64 cookie = ++NextCookie; // actor member; fresh for every attempt
    Send(Backend, new TEvWork(std::move(payload)), 0, cookie);
    auto done = co_await ActorWaitForEvent<TEvWorkDone>(cookie);
    Send(replyTo, new TEvDone(done->Get()->Value), 0, replyCookie);
}

void Handle(TEvCancel::TPtr ev) {
    if (auto it = InFlight.find(ev->Get()->RequestId); it != InFlight.end()) {
        it->second.Scope.Cancel();
    }
}
```

Store ids and owning values in the frame, not pointers into `InFlight`. After
every wait, look the slot up again if you still need it. A replacement protocol
can cancel the old request and await an explicit completion notification before
reusing its key. Never overwrite its scope immediately after `Cancel`: its
deferred cleanup could erase the replacement. Use a fresh wire cookie per
attempt so a late old reply cannot complete a replacement request. StateFunc
must tolerate replies whose waits have ended.

### One background loop per key

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

```cpp
struct TWatch {
    TAsyncCancellationScope Scope;
    ui32 Refs = 0;
};
THashMap<ui64, TWatch> Watches; // key = resource id

void Handle(TEvWatch::TPtr& ev) {          // sync handler: TPtr&
    const ui64 id = ev->Get()->Id;
    if (auto it = Watches.find(id); it != Watches.end() && !it->second.Refs) {
        // Application-defined reply: retry after the old loop has unwound.
        Send(ev->Sender, new TEvWatchRetry, 0, ev->Cookie);
        return;
    }
    auto& w = Watches[id];
    ++w.Refs;
    if (w.Refs == 1) {
        WatchLoop(id);                     // starts a sibling root task
    }
}

void Handle(TEvUnwatch::TPtr& ev) {         // sync handler
    auto it = Watches.find(ev->Get()->Id);
    if (it != Watches.end() && it->second.Refs && --it->second.Refs == 0) {
        it->second.Scope.Cancel();
    }
}

void WatchLoop(ui64 id) {                   // void + co_await => TActorTask
    Y_DEFER { Watches.erase(id); };
    auto it = Watches.find(id);
    Y_ABORT_UNLESS(it != Watches.end());
    it->second.Scope = co_await TAsyncCancellationScope::WithCurrentHandler();

    const ui64 cookie = ++NextCookie; // fresh for this subscription incarnation
    for (;;) {
        Send(Source, new TEvSubscribe(id), IEventHandle::FlagTrackDelivery, cookie);
        auto ev = co_await ActorWaitForEvent<IEventHandle>(cookie);
        ApplyUpdate(id, ev);               // re-find Watches[id] if needed
    }
}
```

`Y_DEFER` erases the key when the loop unwinds. `Scope.Cancel()` from
`TEvUnwatch` is the stop signal; new watches must retry while that cancellation
is pending. `ApplyUpdate` is protocol-specific: it must distinguish updates
from `TEvUndelivered`, validate the sender, and implement its retry or exit
policy. Receiving nondelivery alone does not cancel a coroutine. This is
the tablet-resolver pattern: one `TabletStateSubscriptionLoop` per subscribed
actor, started from `SubscribeTabletState` only if the map has no entry yet.

Do not write `co_await WatchLoop(id)`: a `void` loop is not `async<T>`. Do not
hold a pointer/iterator to the map entry across a wait.

### Fan-out with bounded lifetime

Use WithTaskGroup when a parent must not finish until all children either
finish or unwind. Do not emulate detached children by calling several void
methods; those become unrelated root tasks.

### External callback API

- If callback completion is guaranteed on this actor, adapt it with
  WithAsyncContinuation.
- If completion may be off-thread, Send an event or expose a generic awaiter.
- If one completion wakes many actor-local waiters, Send once and fan out with
  continuations/TAsyncEvent inside StateFunc.

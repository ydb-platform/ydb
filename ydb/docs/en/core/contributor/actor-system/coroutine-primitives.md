# Coroutine Primitives

The stackless actor library provides waits, notifications, deadlines, cancellation scopes, and task groups that resume under the owning mailbox. This reference assumes the frame and lifetime rules in [Coroutine Actors](coroutine-actors.md). For off-thread completion, see [Coroutine Integration](coroutine-integration.md).

## Choosing an await primitive

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

## ActorWaitForEvent

```cpp
const ui64 cookie = ++NextCookie;
Send(peer, new TEvRequest, 0, cookie);
auto ev = co_await ActorWaitForEvent<TEvReply>(cookie);
```

Contract:

- matches GetTypeRewrite plus Cookie; it does not match Sender;
- `ActorWaitForEvent<IEventHandle>` matches any non-system user event with that
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

## AsyncSleep and AsyncYield

```cpp
co_await AsyncYield();
co_await AsyncSleepFor(TDuration::MilliSeconds(10));
co_await AsyncSleepUntil(monotonicDeadline);
```

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

## WithAsyncContinuation

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

```cpp
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
            // This example's allocator returns one ID per request, so each
            // cache miss needs its own allocation request.
            Send(TxAllocator, new TEvAllocateTxId);
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
```

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

## TAsyncEvent

TAsyncEvent is an actor-local, non-sticky notification queue:

```cpp
bool notified = co_await Changed.Wait();
Changed.NotifyOne();
Changed.NotifyAll();
```

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

## WithTimeout and WithDeadline

```cpp
bool completed = co_await WithTimeout(
    TDuration::Seconds(1),
    DoVoidWork());

std::optional<int> value = co_await WithDeadline(
    deadline,
    DoValueWork());
```

For an `async<void>` child the result is bool; for `async<T>` it is `optional<T>`.
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

## Cancellation scopes

TAsyncCancellationScope cancels selected child operations independently from
the actor's root cancellation.

**Its destructor does not cancel work.** Call Cancel explicitly. Cancel is
sticky/idempotent at scope level; a sink attached after cancellation is
cancelled immediately. Cancel and live-sink manipulation are actor-local.

### Wrap

```cpp
TAsyncCancellationScope Scope; // actor member; outlives wrapped work

void Handle(TEvStart::TPtr ev) {
    bool completed = co_await Scope.Wrap([this]() -> async<void> {
        co_await WaitForWork();
    });
    // Inspect completed here if this handler itself was not cancelled.
}

void Handle(TEvCancel::TPtr& ev) {
    Scope.Cancel(); // another turn can run while the first handler waits
}
```

`Wrap<void>` returns bool and `Wrap<T>` returns `optional<T>`. false/nullopt means the
scope's cancellation was confirmed. The request can lose to normal completion,
which then returns success. Cancellation of the caller/root normally unwinds
past Wrap instead of returning false.

An already-cancelled scope still starts the lazy body. Its synchronous prefix
can run, then it should unwind at its first cancellation-aware suspension.

### Attaching the whole root handler

Inside a top-level void handler only:

```cpp
Request.Scope =
    co_await TAsyncCancellationScope::WithCurrentHandler();
```

The returned scope contains that root handler as a sink. A later
Request.Scope.Cancel() requests cancellation of the whole handler. Use it
directly in the root; it relies on the root promise's special await_transform.

This is useful for actor-owned keyed requests: store a scope beside request
state, cancel it on duplicate/explicit cancel, and let an RAII guard erase the
state and notify completion. Since cancellation is deferred, a replacement may
need to await an explicit Finished TAsyncEvent before reusing the key.

### WrapShielded and InterceptCancellation

WrapShielded prevents caller cancellation from entering the wrapped child until
the child completes. It does not clear the caller's latched cancellation, and
explicit scope.Cancel still cancels the child. Shielding can delay PassAway
indefinitely; reserve it for cleanup that truly must finish.

InterceptCancellation(body, onCancel) is the advanced hook for cleanup that
must run when caller cancellation arrives. onCancel may return:

- void or true to propagate cancellation;
- false to decline propagation;
- `async<void>` or `async<bool>` for awaited cancellation handling.

An exception from onCancel is rethrown after inner cleanup. Prefer simple RAII
unless asynchronous/vetoing cancellation is genuinely required.

## Task groups

WithTaskGroup provides structured cooperative fan-out. Children are `async<T>`
frames on the same actor, not threads.

```cpp
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
```

Semantics and corners:

- Add returns a monotonically assigned child index.
- Add stores decayed callback/argument copies and schedules startup on the
  runnable queue; the callback does not run recursively.
- If the group body returns before a scheduled child starts, that callback may
  never run.
- Results queue in completion order.
- Next consumes a value and rethrows a child exception.
- NextResult returns `TTaskGroupResult<T>` with GetIndex plus value/exception;
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

## TAsyncLowPriorityQueue

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

```cpp
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
```

A single `co_await AsyncYield()` is enough when only one frame needs to breathe
and you do not care about a shared policy.

All waiters must belong to the same actor. The queue must outlive them;
destroying it with waiters aborts. Cancellation unlinks a waiter and may disarm
or reuse the pending self-event.

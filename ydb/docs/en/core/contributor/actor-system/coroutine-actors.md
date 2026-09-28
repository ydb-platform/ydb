# Coroutine Actors

This section describes the stackless NActors async library in the current YDB
tree. It is both a usage guide and a compact correctness contract. The legacy
stackful TActorCoro API is a different mechanism and is intentionally out of
scope.

Primary headers are under `ydb/library/actors/async/`:

- [async.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/async.h)
- [wait_for_event.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/wait_for_event.h)
- [continuation.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/continuation.h)
- [event.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/event.h)
- [sleep.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/sleep.h) and [yield.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/yield.h)
- [timeout.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/timeout.h)
- [cancellation.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/cancellation.h)
- [task_group.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/task_group.h)
- [low_priority.h](https://github.com/ydb-platform/ydb/blob/main/ydb/library/actors/async/low_priority.h)

Include async.h in the translation unit that defines a coroutine actor method.
It provides the coroutine_traits specialization that turns an actor member
returning void into a top-level actor task.


Read [Coroutine Primitives](coroutine-primitives.md) for wait APIs, [Coroutine Integration](coroutine-integration.md) for external completion and patterns, and [Failure and Shutdown](failure-and-shutdown.md) for cancellation and RAII behavior. These rules build on the [Actor System Guide](index.md).

## The compact contract

1. **Coroutines do not add threads.** Actor code remains mailbox-serialized.
   Several frames may be suspended, but only one actor turn or continuation
   executes at a time.

2. **There are two different frame owners.** A void actor coroutine is a
   top-level task registered with the actor. An `async<T>` is a lazy child frame
   owned by the expression that awaits it.

3. **Top-level parameters are values.** Every explicit parameter of a void
   actor coroutine must be a non-reference type. In particular, an async hFunc
   handler takes TEv::TPtr ev, not TEv::TPtr&.

4. **A top-level call is immediate and unstructured.** It runs synchronously
   until its first real suspension and returns no handle. Calling another void
   coroutine does not create a child relationship; use `async<T>` and co_await
   when sequencing and ownership matter.

5. **An `async<T>` call is lazy.** The call creates a suspended frame; its body
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

## Two frame kinds

| Property | Top-level actor coroutine | Nested `async<T>` |
|---|---|---|
| Signature | Non-static IActor member returning void | Function/member/lambda returning `async<T>` |
| Start | Immediately, initial_suspend is suspend_never | Lazily, initial_suspend is suspend_always |
| Return object | None | A non-copyable, normally non-movable `async<T>` |
| Owner | Actor's TActorTask list | Direct co_await expression / decorator |
| Actor lifetime | PassAway waits for it | Lives under the top-level task that awaits it |
| Result | None | T or void; exception rethrown at co_await |
| Cancellation | Root cancellation source | Inherited from the awaiting parent |

### Top-level frame lifecycle

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

```cpp
void Handle(TEvRequest::TPtr ev) {
    ChildRoot();              // sibling TActorTask; Handle does not wait
    co_await SomethingElse();
}

void ChildRoot() {            // also a top-level void coroutine
    co_await Work();
}
```

Prefer a nested `async<T>` that Handle co_awaits:

```cpp
void Handle(TEvRequest::TPtr ev) {
    co_await Child();
}

async<void> Child() {
    co_await Work();
}
```

### Nested frame lifecycle

Calling an `async<T>` function:

1. creates its frame and stores its parameter copies/references;
2. stops at initial_suspend before executing the body;
3. returns an `async<T>` object that owns the frame handle.

Then one of two things happens:

- if the async object is discarded, its destructor destroys the frame; body
  locals were never constructed and the body never runs;
- if directly awaited, the await expression installs the actor, cancellation
  source, and parent continuation, then transfers execution into the child.

On child completion, final suspend transfers back to the parent. The promise
retains the value or exception until await_resume extracts it. At the end of
that co_await full-expression the owning async object destroys the child frame.
An exception stored by a nested child is rethrown in the parent at co_await.

`async<T>` is deliberately not a general future or detachable task. Do not use
UnsafeMove in ordinary application code; it exists for library combinators.

## Parameters, captures, and frame-owned data

The top-level specialization is selected only for:

```cpp
template<IsActorSubClassType T, IsNonReferenceType... Args>
struct std::coroutine_traits<void, T&, Args...>;
```

Therefore all explicit arguments must be values:

```cpp
void Handle(TEvRequest::TPtr ev) { // correct: event ownership enters frame
    co_await Process();
}

void Handle(TEvRequest::TPtr& ev) { // not a stackless actor coroutine promise
    co_await Process();             // compile-time failure
}
```

hFunc passes its TAutoPtr lvalue to the by-value handler, transferring event
ownership. The hidden this reference is expected and is not one of Args.
Coroutine Bootstrap overloads must likewise take explicit parameters by value.
Prefer `Bootstrap()` or `Bootstrap(TActorId parent)` for a coroutine. Copying a
`TActorContext` does not extend the lifetime of the activation it refers to;
never use that context after suspension. Install Become before Bootstrap's
first suspension.

Nested `async<T>` functions may have reference parameters, but those are ordinary
borrows:

```cpp
async<void> Use(const TString& value); // caller must keep value alive
```

A by-value top-level event handle remains memory-safe across suspension because
the frame owns it. It may nevertheless retain a large payload for a long time.
Extract the small fields that are needed and ev.Reset() before a long wait.

Coroutine lambdas have an extra lifetime trap: their frames generally borrow
the lambda closure through its this pointer. These are safe:

```cpp
auto operation = [value]() -> async<void> {
    co_await Use(value);
};
co_await operation(); // named closure outlives the await

co_await WithTimeout(TDuration::Seconds(1), [value]() -> async<void> {
    co_await Use(value);
}); // closure temporary lives through this co_await full-expression
```

Do not create/store the returned lazy async or a decorator in one full-expression
and await it in another. TTaskGroup::Add is different: it stores a decayed copy
of the callback and arguments until the child finishes.

In general, avoid coroutine lambdas as much as possible.

---

## Minimal request/reply actor

The event definitions are omitted; the important details are value parameters,
unique cookies, a nested helper, timeout handling, stale-reply dispatch, and a
dying state.

```cpp
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
```

Each TEvStart starts an independent top-level task. Those tasks overlap while
waiting but never execute simultaneously. ActorWaitForEvent cancels promptly,
so this particular timeout can complete cooperatively. A different child
awaiter may not.

ActorWaitForEvent matches only type and cookie, not Sender. The example assumes
only Backend uses these reply cookies. If sender authentication matters, route
replies through StateFunc or a protocol-specific stream that validates it.

## Execution and resumption

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
(FlagSystemMessage). If that event is destroyed unhandled while its Item is
still armed, ~TEvResumeRunnable calls Item->Run(nullptr). If Item was set to
nullptr to disarm the event, destruction does nothing. The library may disarm
actor-local pending events this way; scheduler-owned events need a separate
bridge because their destruction can race actor execution.

### Interleaving rules

Treat every co_await that may suspend as:

```text
save frame state
allow arbitrary later actor turns
resume under the mailbox
revalidate assumptions
```

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

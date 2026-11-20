# Failure and Shutdown

Actor shutdown is an explicit transition that can outlive the handler that starts it. This page describes cancellation, cleanup, and the relationship between actor lifetime and coroutine frames. Read [Events and Messaging](events-and-messaging.md) for delivery failures and [Coroutine Actors](coroutine-actors.md) for frame ownership.

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

Nested `async<T>` frames belong to the top-level task that awaits them; they are
not separately registered actor tasks.

The deferred interval matters. Normal StateFunc handlers can still run. A new
top-level coroutine can run its synchronous prefix but starts already
cancelled, and should unwind at its first cancellation-aware suspension. A
later TEvPoison that still reaches StateFunc and calls PassAway again aborts. If an actor may have
live tasks, switch to a state that drops or deliberately handles late events
before calling PassAway, as in the minimal example. A coroutine initiating
death should do:

```cpp
Become(&TThis::StateDying);
PassAway();
co_return;
```

A task parked on an awaiter that does not support cancellation, such as a
TFuture, can keep the actor registered indefinitely. Actor-system/mailbox teardown is the emergency
path: DestroyActorTasks force-destroys remaining coroutine frames and then the
actor. It does not run user code after an await, but C++ destruction of live
frame parameters and locals still occurs, so RAII destructors run.

PassAway only requests cancellation; it does not jump out of the calling
handler, so do no shutdown-sensitive work after it. A normal completion already
in flight may win the race, so make late post-await work check a dying flag or
generation and remain harmless. TActorSystem::Stop is hard teardown: it does not
gracefully Poison every actor. Children and scheduled events also do not
automatically die when a parent calls PassAway.

PassAwayGuard is a move-only RAII guard whose destructor calls PassAway. It is
useful only when that guard is the single owner of the actor's death transition;
combining it with another Poison/death path risks the forbidden second call.

Mailbox aliases created with IActor::RegisterAlias are removed when the actor
detaches. TActorSystem service-map entries are separate and are not removed
with the actor; clear or replace them explicitly.

## Cancellation protocol

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

## RAII: what runs and when

| Exit path | Body continues? | Live frame destructors run? | Actor TLS reliable? |
|---|---|---|---|
| Normal co_return | Yes, through normal scopes | Yes | Yes during the turn |
| Nested exception | Stack unwinds to a catch/parent | Yes | Yes during the turn |
| Confirmed cancellation | No code after cancelled await | Yes | Normally during actor execution |
| Never-awaited `async<T>` dropped | Body never starts | Promise/parameters only; no body locals | Not required |
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

### Exceptions

A nested `async<T>` stores an exception and rethrows it at the parent's
await_resume, so normal try/catch and RAII apply:

```cpp
try {
    co_await Child();
} catch (const TExpectedError& e) {
    // protocol error handling
}
```

An actor opts into exception handling by deriving from
`TActorWithExceptionHandler<TBase>`, where `TBase` is its existing actor base,
for example `TActor<TDerived>` or `TActorBootstrapped<TDerived>`. This adds a
link to the actor inheritance chain, not a separate handler interface.
`IActor::HasExceptionHandler()` returns false by default; the opt-in base
returns true. `Receive` checks this virtual method before dispatch and needs
neither RTTI nor a cached type check.

The actor implements `OnUnhandledException(const std::exception&)` and returns
true for a handled exception or false to rethrow it. Its implementation may be
private: invocation through the virtual contract does not depend on member
access. The base's `std::exception_ptr` overload forwards standard exceptions
to that method and declines other thrown objects. Override that overload to
handle non-standard exceptions. Cancellation never enters this catch block.
Overriding a handler method on an ordinary actor alone does not opt into catching.

Actors without an exception handler dispatch events outside Receive's catch
block. This avoids unwinding the actor handler merely to rethrow an exception
that cannot be handled. On runtimes that terminate before unwinding an uncaught
exception, the original actor frames remain available to crash stack collection.
Actors opting into exception handling retain the catch/handler path; if the
handler declines the exception, its original frames have already been unwound.

A declined exception is rethrown through the non-inlined virtual method
`TActor<TDerived>::RethrowUnhandledException()`. The fatal stack therefore
identifies the CRTP actor type even after its state function has unwound. This
does not reconstruct the original throw site or its local variables. A subclass
that reuses `TActor<Base>` is identified as `Base`; actors deriving directly from
`IActor` use its generic rethrow method. An exception thrown by a standard
exception handler does not replace the original exception being rethrown.

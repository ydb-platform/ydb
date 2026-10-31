# Generic async operation lifecycle (D1)

This standalone MiniKQL-layer library is the task-scoped lifecycle foundation for
DQ async computation. It has no dependencies on actors, computation graphs,
WASM, transport or an engine. It is not wired into query execution yet.

## Owner and provider contract

The task owner serializes `RegisterProvider`, `Submit`, `Cancel`, `TakeReady`,
`ExpireDeadlines` and `Shutdown`. `ICompletion` methods can run on any thread;
`GetStats` is also thread-safe. No provider or wakeup callback runs under the
registry mutex. Start, Cancel and provider destruction must not wait for I/O.

The owner registers a provider instance per name. Each accepted call receives a
process-local context generation and a never-reused sequence number. A token is
bound to its original registry; IDs from another context cannot cancel its work.
These IDs are not persistent or distributed query identities.

Requests and results are owned, allocator-independent bytes, encoded according to
the provider's operation contract. A provider must not put allocator-bound
MiniKQL values into its callbacks. Generic SQL type metadata/codecs are D2 work.
The library neither resolves credentials nor logs payloads.

A completion can arrive during Start, after Cancel, or after context destruction.
Only the first terminal outcome wins: success, error, cancellation or timeout.
Deadlines use a supplied thread-safe monotonic clock. The host arms the earliest
`NextDeadline()` and calls `ExpireDeadlines()` on the owner context; the library
creates no polling loop, worker thread or timer. Completion/cancellation at or
after the deadline becomes Timeout, unless an earlier terminal outcome already
won. Host rearming is needed after owner operations that change pending calls;
an obsolete timer is harmless.

## Logical completion and physical cleanup

`Complete`/`Fail` publish an outcome; `Release` acknowledges physical cleanup.
Providers retain a completion reference until all resources belonging to the
call have been released. Dropping the last token acknowledges cleanup and
reports Abandoned if the provider never published an outcome. A thrown Start
reports StartFailed and requests cancellation if it won the terminal race.

Admission reserves an operation slot and bytes for Operation + input Payload +
MaxResponseBytes. Credits stay held until **both** physical cleanup and result
consumption. Cancellation cannot release the reservation for a still-live
request. Ready-but-unconsumed results also retain it. Oversized success/error
payloads become ResponseTooLarge; providers must enforce MaxResponseBytes while
receiving data, before constructing a response, not rely on this last check to
bound transient buffers.

These are conservative payload/operation limits, not a full allocator quota:
provider buffers and metadata are the provider's responsibility, registry entry
count is bounded by MaxOperations, and engine quota integration is D3 work.
Taking results transfers their byte ownership/accounting to the consumer.
Shared byte-string storage must be accounted conservatively by its owners.

## Readiness and shutdown

Readiness notifications coalesce until `TakeReady` drains results and rearms
under the same mutex. A racing publication either belongs to that drain or
requests a new wakeup. Cleanup returning credits after result consumption also
requests a wakeup: the owner retries admission even when the ready queue is empty.
Credits returned during `TakeReady` are already visible to that owner turn. The wakeup callback must only enqueue a lifetime-safe,
nonthrowing notification carrying the generation; it must not execute graph
computation inline. The injected clock must likewise remain valid for outstanding
tokens and must not throw.

Shutdown closes admission, detaches notifications and requests cancellation of
unfinished calls without waiting for cleanup. Results can still be drained.
The detached wakeup's captures are destroyed outside the registry mutex, after
cancellation dispatch: releasing their backend state may destroy completion tokens
and reenter cleanup, even when those tokens already acknowledged Release.
A notification copied before shutdown can arrive afterwards: the receiver must
use a lifetime-safe handle and ignore obsolete generations. Context destruction
discards results; outstanding tokens keep input/reservations alive until cleanup.
The shared registry state never owns providers, preventing provider/token cycles.
A provider must independently preserve backend state used by outstanding callbacks:
provider destruction may precede physical cleanup and cannot wait for it.

D1 does not implement continuation frames, compiler effects, CPU quanta, batching,
checkpoint replay, or FQ/KQP bridges. In D2/D3, a continuation that exhausts its CPU
budget with runnable work must explicitly arm a coalesced rerun: completion
readiness alone is not a substitute. A truly waiting task must not self-poll.

The D2 compiler/runtime is in [compute](compute/README.md), a separate target
above this standalone lifecycle library. Engine bridges are still pending D6.

The D3 owner-driven batching, resource and fairness layer is described in
[SCHEDULER.md](SCHEDULER.md). It builds on this lifecycle without adding WASM,
transport, SQL or engine dependencies.

# PDisk I/O library

This library provides device I/O primitives used by PDisk and DDisk. It owns
backend mechanics, buffers and completion delivery. PDisk owns device/log/chunk
policy; DDisk and its PersistentBuffer own their request and record lifetimes.
See the [storage overview](../../docs/en/core/contributor/distributed-storage.md)
and [DDisk contracts](../../docs/en/core/contributor/distributed-storage/ddisk.md)
for those layers.

## Source map

| Source | Responsibility |
| --- | --- |
| [aio.h](aio.h), [aio.cpp](aio.cpp) | Asynchronous I/O interfaces and backend factory |
| [aio_linux.cpp](aio_linux.cpp), [aio_mtp.cpp](aio_mtp.cpp) | Linux native AIO and the platform fallback backend |
| [aio_map.cpp](aio_map.cpp), [sector_map.h](sector_map.h) | In-memory sector-map I/O used by emulation/tests |
| [uring_router.h](uring_router.h), [uring_router.cpp](uring_router.cpp) | One io_uring and its dedicated I/O thread |
| [uring_router_client.h](uring_router_client.h) | Submit-only interface and shared router configuration |
| [uring_router_backend.h](uring_router_backend.h) | Backend seam used to isolate liburing calls in tests |
| [uring_operation.h](uring_operation.h), [uring_operation.cpp](uring_operation.cpp) | Operation lifetime, scalar/scatter-gather buffers, result and retry cursor |
| [buffers.h](buffers.h), [buffer_pool.h](buffer_pool.h) | Aligned buffers and pooling |
| [file_params.h](file_params.h), [drivedata.h](drivedata.h), [device_type.h](device_type.h) | File/device geometry and drive information |
| [device_io_sample.h](device_io_sample.h) | Timing sample exchanged with device estimation |
| [ya.make](ya.make) | Platform selection; io_uring implementation is built on Linux |

## TUringRouter ownership and setup

`TUringRouter` owns the duplicated file handle passed to its constructor, its
ring, wake eventfd and dedicated I/O thread. The actor-system pointer is
borrowed and must remain usable through `StopSync()`, including all terminal
callbacks. The router is neither an actor nor the owner of the device's PDisk
allocation.

In the DDisk integration, PDisk lazily creates and starts one shared router for
its device when the first `TEvYardInit` requests an `IUringRouterClient`. That
request carries the DDisk `IdleSpinUs` setting; because the router is shared,
the first requesting DDisk selects this value for that PDisk incarnation.
Later DDisk slots and their PersistentBuffer children share submit-only client
references to the same router. They cannot register resources, start it, or
stop it.

Setup runs on one thread before concurrent submission:

1. Probe availability when choosing the backend. Ring creation first tries
   the modern single-issuer/deferred-task-run flags and then a plain ring.
2. Construct the router and record any `RegisterFile`, `RegisterBuffers` and
   `SetSampleSink` requests.
3. Call `Start()` once. The dedicated I/O thread enables the ring and performs
   registrations; `Start()` waits for that work before opening admission.
4. Check registration success and error accessors before using fixed I/O.

The `iovec` registration array must survive until `Start()` returns. Registered
buffer memory must survive the registration and every operation that uses it.
Failed fixed-file registration permits ordinary file-descriptor I/O; failed
buffer registration does not permit `ReadFixed` or `WriteFixed`.

## Submission and completion

Concurrent producers call `Submit`, `Read`, `Write`, `ReadFixed` or `WriteFixed`.
They publish into an MPSC queue; only the I/O thread prepares/submits SQEs and
reaps CQEs. Queue-depth pressure delays queued work and does not reject an
otherwise admitted submission. `Flush()` is a compatibility no-op because the
I/O thread owns batching.

The lifetime boundary is the return value of submission:

- `true`: admission transferred the operation to the router. Its completion
  callback can run **before the submission call returns**. Release a smart
  pointer before passing its raw pointer, and do not access the operation
  after a successful call.
- `false`: the router is not started or is stopping/stopped. No callback will
  run for this attempt; the caller retains responsibility for the operation
  and its buffers.

An accepted submission gets exactly one terminal callback before `StopSync()`
returns: `OnComplete()` if it reached the kernel (including error completion),
or `OnDrop()` if shutdown discards it first. The router does not
access the operation after that callback returns, so the callback may free it
or return it to a pool. `GetInflight()` counts accepted queued/submitted work
and callbacks still executing.

`OnComplete()` and `OnDrop()` run on the I/O thread outside actor activation.
They must be `noexcept`, must not use `TActivationContext`, and should send
actor messages through the supplied `TActorSystem`. They must not call
`StopSync()` on this router or release its last reference, because synchronous
shutdown would try to join the callback's own thread. Keep callback and optional
sample-sink work short; the single I/O thread also services all other requests.

`GetResult()` exposes the total requested byte count after successful logical
completion, or a negative errno. The router advances the iovec window and
continues positive short I/O internally. A zero-byte result with data remaining
becomes `-EIO`; if shutdown prevents an internal continuation, the operation
completes with `-ECANCELED`. Keep all backing buffers alive until the terminal
callback.

`PrepareScatterGather`/`AddIov` support up to 64 segments on Linux, with 16
stored inline. Fixed-buffer operations use one registered segment. A recycled
operation must call `ResetSubmissionState()` before preparing new I/O; do not
carry its previous offset, result, fixed-buffer index or retry cursor forward.
Alignment requirements come from the opened device and caller contract.

## Shutdown

`StopAsync()` atomically closes admission and returns without waiting. A
submission racing with that transition may still be accepted and will still
receive one terminal callback. Repeated and concurrent `StopAsync()` calls are
supported; concurrent setup and `Start()` calls are not part of the contract.

`StopSync()` closes admission, waits for accepted publishers to finish queue
publication and wake notification, appends a stop sentinel, and joins the I/O
thread. Concurrent synchronous stops are serialized. The I/O thread calls
`OnDrop()` for accepted work that has not reached the kernel, drains submitted
I/O through `OnComplete()`, retires pending wake polls, and fences submitted
work with an `IOSQE_IO_DRAIN` marker. Short-I/O continuations canceled by
shutdown complete with `-ECANCELED`. Normal shutdown has no drain deadline.
Callback state and backing buffers must survive until their terminal callbacks
return; the actor system must remain usable through `StopSync()`.

Before returning, `StopSync()` destroys the ring and closes the duplicated
device handle. The wake eventfd remains valid until router destruction, which
also calls `StopSync()`. Retained initialization responses and submit-only
client references therefore cannot keep the device handle open after
synchronous shutdown; their new submissions are rejected without callbacks.

PDisk owns the router and calls `StopSync()` during shutdown before stopping
its block device and releasing the source descriptor. DDisk and PersistentBuffer
clients cannot stop the shared router. Descriptor closure and device-lock
reacquisition are separate guarantees: explicitly unlocking a device does not
prove that its descriptor was closed. Sampling callbacks capture shared
aggregator ownership directly and must not reference PDisk or its monitoring
object.

A fatal ring/backend failure marks the router broken and closes admission.
PDisk observes that state and reports a device error once so that PDisk and its
slots can be restarted. Clients still wait for their own accepted work. During
synchronous teardown, fatal shutdown paths allow 200 ms for late completions.
If data operations, published SQ entries, wake polls or stop markers remain
unresolved at the deadline, the process aborts before freeing resources whose
ownership may still belong to the kernel. Retaining only the ring and device
handle would not protect caller-owned buffers.

## DDisk integration and fallback boundary

[TPDisk::AttachSharedUringRouter](../../core/blobstorage/pdisk/blobstorage_pdisk_impl.cpp)
handles a DDisk request for direct I/O. On the first request it duplicates the
device handle, probes io_uring, creates and starts the per-device router, and
installs fixed-file registration and a timing sample sink with shared aggregator
ownership. Failure to create the shared router returns no client, so DDisk uses
PDisk raw-event I/O instead.
`ForcePDiskFallback` opts out in the DDisk yard-init request and always selects
that fallback path.

[direct_io_op.cpp](../../core/blobstorage/ddisk/direct_io_op.cpp) owns DDisk's
operation payload, short-I/O counters, critical retries and delivery back to the
actor. Selection of PDisk fallback and translation into PDisk requests belong
to the DDisk caller, not to `TUringRouter`. A plain ring is still io_uring; an
ordinary CQE error is not an automatic switch to PDisk. Changes at this boundary
must preserve sender/cookie, payload ownership and final completion on both paths.

## Tests

The library target is `ydb/library/pdisk_io`; its Linux unit-test target is
`ydb/library/pdisk_io/ut`. Follow repository/personal build rules for invocation.
[uring_router_ut.cpp](ut/uring_router_ut.cpp) uses temporary files and covers:

- Queue overload, multiple producers, wake-after-idle and normal I/O errors.
- Fixed buffers, scatter/gather, retry cursor behavior and timing samples.
- Submission before/after admission, submission racing with asynchronous stop,
  concurrent synchronous stops, ring and descriptor retirement, and synchronous
  stop or destruction waiting for a running callback.

Shared [uring_router_test_peer.h](uring_router_test_peer.h) exposes scripted
issuer phases and retirement snapshots. Its instance-local, immutable hook
bundle is installed before `Start()` and supplies publisher, callback and stop
barriers. Keep hook state alive until callbacks retire; cleanup must release
blocked threads before destroying the fixture. The private backend supplies a
monotonic clock for deterministic fatal-drain deadlines.

[uring_test_support.h](uring_test_support.h) provides `RequireUring()`. Native
tests using this helper explicitly print `SKIP` when kernel or sandbox
restrictions prevent io_uring setup. Pass `--test-param=require_io_uring=1` to
make an unavailable native backend fail the capability check. Scripted-backend
success does not establish native-kernel coverage.

DDisk/PB tests in `ydb/core/blobstorage/ddisk/ut` cover the caller layer and
PDisk fallback. Shared [ddisk_actor_test_peer.h](../../core/blobstorage/ddisk/ddisk_actor_test_peer.h)
controls the forced-destructor clock;
[blobstorage_pdisk_test_peer.h](../../core/blobstorage/pdisk/blobstorage_pdisk_test_peer.h)
installs router configuration under `StateMutex` before router creation.
[node_warden_test_peer.h](../../core/blobstorage/nodewarden/node_warden_test_peer.h)
supports scripted Warden restart checks. The NodeWarden unit target also contains
`TRequestedPDiskRestartFixture`, which uses a real file and native callback gates
to exercise the DDisk/PB restart handoff. Library tests alone cannot establish
actor shutdown fencing or PB record durability.

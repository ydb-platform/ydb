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
borrowed and must remain usable until the last router owner releases it and all
terminal callbacks have returned. The router is neither an actor nor the owner
of the device's PDisk allocation.

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

An accepted submission gets exactly one terminal callback before final router
destruction returns: `OnComplete()` if it reached the kernel (including error
completion), or `OnDrop()` if shutdown discards it first. The router does not
access the operation after that callback returns, so the callback may free it
or return it to a pool. `GetInflight()` counts accepted queued/submitted work
and callbacks still executing.

`OnComplete()` and `OnDrop()` run on the I/O thread outside actor activation.
They must be `noexcept`, must not use `TActivationContext`, and should send
actor messages through the supplied `TActorSystem`. They must not release the
last router reference, because its destructor would try to join the callback's
own thread. Keep callback and optional sample-sink work short; the single I/O
thread also services all other requests.

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

The last shared-owner release runs synchronous shutdown in the router
destructor. It appends a stop sentinel, calls `OnDrop()` for accepted work that
has not reached the kernel, drains submitted I/O through `OnComplete()`, retires
any pending wake poll, and uses an `IOSQE_IO_DRAIN` marker before joining the
I/O thread and tearing down the ring. Callback state, backing buffers, and the
actor system must therefore survive until each client's accepted I/O has
finished and final router destruction has returned.

PDisk owns router creation and the non-blocking stop decision. During PDisk
shutdown it detaches device sampling. If PDisk holds the only router reference,
it calls `StopAsync()` and releases it, so final destruction performs the
synchronous cleanup. If DDisk or PersistentBuffer clients still hold
references, PDisk deliberately neither closes admission nor waits: an old slot
may continue until its owner-stamped PDisk request detects the stale round.
PDisk then releases its reference during destruction, and the last client to
release the router performs the drain/drop above. The duplicated device handle
remains open until then, so a replacement PDisk waits for the old holder when
acquiring the device lock.

A fatal ring/backend failure marks the router broken and closes admission.
PDisk observes that state and reports a device error once so that PDisk and its
slots can be restarted. Clients still wait for their own accepted work. If a
fatal failure leaves any operation potentially owned by the kernel, final
destruction aborts instead of freeing storage whose ownership is unresolved.

## DDisk integration and fallback boundary

[TPDisk::AttachSharedUringRouter](../../core/blobstorage/pdisk/blobstorage_pdisk_impl.cpp)
handles a DDisk request for direct I/O. On the first request it duplicates the
device handle, probes io_uring, creates and starts the per-device router, and
installs fixed-file registration and device-timing sampling. Failure to create
the shared router returns no client, so DDisk uses PDisk raw-event I/O instead.
`ForcePDiskFallback` opts out in the DDisk yard-init request and always selects
that fallback path.

[direct_io_op.cpp](../../core/blobstorage/ddisk/direct_io_op.cpp) owns DDisk's
operation payload and delivery back to the actor. Selection of PDisk fallback
and translation into PDisk requests belong to the DDisk caller, not to
`TUringRouter`. A plain ring is still io_uring; an ordinary CQE error is not an
automatic switch to PDisk. Changes at this boundary must preserve
sender/cookie, payload ownership and final completion on both paths.

## Tests

The library target is `ydb/library/pdisk_io`; its Linux unit-test target is
`ydb/library/pdisk_io/ut`. Follow repository/personal build rules for invocation.
[uring_router_ut.cpp](ut/uring_router_ut.cpp) uses temporary files and covers:

- Queue overload, multiple producers, wake-after-idle and normal I/O errors.
- Fixed buffers, scatter/gather, retry cursor behavior and timing samples.
- Submission before/after admission, submission racing with asynchronous stop,
  concurrent stops, and destruction waiting for a running callback.

Kernel or sandbox restrictions may prevent io_uring setup; distinguish that
from an I/O correctness failure. DDisk/PB integration tests in
`ydb/core/blobstorage/ddisk/ut` cover the caller layer and PDisk fallback.
Library tests alone cannot establish actor shutdown fencing or PB record
durability.

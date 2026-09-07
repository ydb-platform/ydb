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
| [uring_operation.h](uring_operation.h), [uring_operation.cpp](uring_operation.cpp) | Operation lifetime, scalar/scatter-gather buffers, result and retry cursor |
| [buffers.h](buffers.h), [buffer_pool.h](buffer_pool.h) | Aligned buffers and pooling |
| [file_params.h](file_params.h), [drivedata.h](drivedata.h), [device_type.h](device_type.h) | File/device geometry and drive information |
| [device_io_sample.h](device_io_sample.h) | Timing sample exchanged with device estimation |
| [ya.make](ya.make) | Platform selection; io_uring implementation is built on Linux |

## TUringRouter ownership and setup

`TUringRouter` takes a borrowed file handle and actor-system pointer. Their
owners must keep them usable through `Stop()`, including completion callbacks.
The router owns the ring, wake eventfd and dedicated I/O thread. It is neither
an actor nor the owner of the device's PDisk allocation.

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

An accepted submission gets exactly one `OnComplete()` callback before
`Stop()` returns. The router does not access the operation after that callback
returns, so the callback may free it or return it to a pool. `GetInflight()`
counts accepted queued/submitted work and callbacks still executing.

`OnComplete()` runs on the I/O thread outside actor activation. It must be
`noexcept`, must not use `TActivationContext`, and should send actor messages
through the supplied `TActorSystem`. It must not call `Stop()` on this router,
which would try to join the callback's own thread. Keep callback and optional
sample-sink work short; the single I/O thread also services all other requests.

`GetResult()` exposes the CQE byte count or negative errno. The router does
not turn short I/O into a complete logical request. The operation/caller
decides whether to advance its iovec window and resubmit, or report failure.
Each accepted resubmission has its own callback and can be rejected after
shutdown closes admission. Keep backing buffers alive across that decision.

`PrepareScatterGather`/`AddIov` support up to 64 segments on Linux, with 16
stored inline. Fixed-buffer operations use one registered segment. A recycled
operation must call `ResetSubmissionState()` before preparing new I/O; do not
carry its previous offset, result, fixed-buffer index or retry cursor forward.
Alignment requirements come from the opened device and caller contract.

## Shutdown

`Stop()` atomically closes admission, waits for producers already publishing
to finish, and appends a stop sentinel behind accepted work. The I/O thread
drains queued requests and completions, retires any pending wake poll, and
uses an `IOSQE_IO_DRAIN` marker before exiting. `Stop()` joins that thread and
tears down the ring only after callbacks have finished.

Repeated and concurrent `Stop()` calls are supported. Concurrent setup and
`Start()` calls are not part of the contract. Ordinary graceful shutdown uses
`OnComplete`, including error completions; `OnDrop` is an operation cleanup
hook for explicit abortive/failure ownership paths, not the graceful drain
mechanism. Do not destroy callback state or the actor system before the drain
finishes.

## DDisk integration and fallback boundary

[TDDiskActor::InitUring](../../core/blobstorage/ddisk/ddisk_actor_boot.cpp)
chooses io_uring when enabled, a raw disk handle/format is available, and
`Probe` succeeds. It requests fixed-file registration and installs device
timing sampling. The actor otherwise uses PDisk raw-event I/O.

[direct_io_op.cpp](../../core/blobstorage/ddisk/direct_io_op.cpp) owns DDisk's
operation payload, short-I/O completion handling and delivery back to the
actor. Selection of PDisk fallback and translation into PDisk requests belong
to the DDisk caller, not to `TUringRouter`. A plain ring is still io_uring;
an ordinary CQE error is not an automatic switch to PDisk. Changes at this
boundary must preserve sender/cookie, payload ownership and final completion
on both paths.

## Tests

The library target is `ydb/library/pdisk_io`; its Linux unit-test target is
`ydb/library/pdisk_io/ut`. Follow repository/personal build rules for invocation.
[uring_router_ut.cpp](ut/uring_router_ut.cpp) uses temporary files and covers:

- Queue overload, multiple producers, wake-after-idle and normal I/O errors.
- Fixed buffers, scatter/gather, retry cursor behavior and timing samples.
- Submission before/after admission, submission racing with stop, concurrent
  stops, and stop waiting for a running callback.

Kernel or sandbox restrictions may prevent io_uring setup; distinguish that
from an I/O correctness failure. DDisk/PB integration tests in
`ydb/core/blobstorage/ddisk/ut` cover the caller layer and PDisk fallback.
Library tests alone cannot establish actor shutdown fencing or PB record
durability.

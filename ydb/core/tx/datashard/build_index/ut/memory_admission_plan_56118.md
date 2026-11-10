# Bounded memory for queued index-build scans

Plan for [issue #56118](https://github.com/ydb-platform/ydb/issues/56118), based on the [profiling investigation](memory_profile_56118.txt).

Introduce separate limits for waiting requests and active scans, and move admission before expensive allocation.

Implemented in the working tree. See [implementation and validation](memory_admission_fix_56118.md) for defaults, compatibility, passing regression suites and post-fix profiles.

## Memory contract

```text
pending scan count         ≤ PendingCountLimit
retained pending bytes     ≤ PendingBytesLimit
active reserved memory     ≤ ActiveMemoryLimit
```

These budgets should fit within the node's memory-controller budget, leaving room for other consumers and allocator overhead. The reported production limit is 16 GB per node; the scan budget must not consume that entire limit.

## 1. Remove unnecessary centroid scratch allocation

Add an explicit assignment-only mode for reshuffle, so it does not allocate `NextClusters`.

Do not base this on `MaxRounds == 0`: [recompute scans also use zero rounds and aggregate vectors](../recompute_kmeans.cpp#L237). This is an immediate reduction, but queue limits are still needed.

## 2. Queue a deferred scan description

Extend the shared executor scan API to accept a factory plus a memory estimate. Before admission, retain only the original request and small identifiers—no constructed scan, centroid scratch arrays, output buffers or read-ahead state.

Avoid closures capturing copied payloads or table references. Move duplicate/sequence checks before construction.

Also defer expensive [scan snapshot preparation](../../../../tablet_flat/flat_executor.cpp#L4548), while preserving the requested MVCC version and a bounded validity lease. Revalidate table, schema and snapshot state when admission arrives.

## 3. Require CPU and memory before invoking the factory

Extend `TScanOptions` and the executor's broker submission to declare nonzero memory. Estimate the peak retained request, centroid state, required scratch arrays, upload buffers/request copies, read-ahead and container overhead.

Enforce buffer growth within that reservation. The measured 28 MiB per active scan is specific to the profiling dataset, not a universal constant.

Add a memory limit for `queue_build_index`, integrated with memory-controller updates. Introduce strict memory admission for this queue: the broker currently [allows oversized tasks when the queue is idle](../../../../tablet/resource_broker.cpp#L627).

A task that cannot fit must receive an explicit outcome instead of bypassing the limit or waiting forever.

## 4. Bound pending requests across the whole node

Add a shared pending-budget reservation used by every DataShard before retaining a queued request. Enforce both count and byte limits; count alone cannot bound large centroid payloads.

Charge retained protobuf storage, descriptor overhead and snapshot-related state conservatively, rather than using serialized size alone. Add a maximum individual request size.

Transfer ownership from the pending reservation to the active reservation without an unaccounted interval.

## 5. Propagate overload to SchemeShard

Queue saturation should produce a retryable admission outcome, not `BUILD_ERROR`.

Add delayed retries with jitter, fairness between builds, and bounded sender windows or byte credits so rejected work does not accumulate in actor mailboxes.

Simply reusing `ABORTED` would cause rapid retries: [its current handler immediately reschedules work](../../../schemeshard/index/build_index__progress.cpp#L4162). Deploy retry support before enabling rejection, with mixed-version compatibility covered.

## 6. Make reservation lifetime explicit

Keep one executor-owned broker reservation through initialization, execution and uploader cleanup. Release it only after the charged memory is freed—not merely when cancellation or poison is sent.

Handle pending cancellation, replacement generations, initialization failure, tablet shutdown and late grants exactly once. Preserve charges for retained container capacity.

## 7. Add bounded-memory regression coverage

Extend the existing reproducers to cover:

- Count and byte saturation independently.
- Large individual requests and oversized tasks on an idle broker.
- Delayed uploads.
- Cancellation and late-grant races.
- Snapshot invalidation.
- Competing builds and retry compatibility.

Assert that heavy construction never happens before admission and that reservations return to zero after cleanup. Re-profile increasing parallelism to verify bounded live memory.

## Delivery order

1. Centroid optimization.
2. Deferred construction with strict memory admission.
3. Pending limits and upstream backpressure.

Each stage gets focused tests. Enable hard limits only after retry compatibility is in place.

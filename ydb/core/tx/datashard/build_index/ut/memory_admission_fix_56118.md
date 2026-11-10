# Index-build memory admission: implementation and validation

Implementation of the [saved plan](memory_admission_plan_56118.md) for
[YDB issue #56118](https://github.com/ydb-platform/ydb/issues/56118).
The [original profiling report](memory_profile_56118.txt) records the unfixed
baseline at revision `b0e1eb15a9962dbba38489a9d7bf46a5fc5acde4`.
Validation date: 2026-11-10. The reported production limit is 16 GB per node.

## Implemented behavior

All six vector scan requests—sample, local, reshuffle, recompute, prefix and
filter—support admission before constructing the scan or preparing its storage
snapshot. Waiting scans retain the original request and a small descriptor.
Duplicate/sequence checks run before heavy construction. The factory revalidates
the table, schema, snapshot and volatile transactions after the broker grant.
Explicit snapshots retain their requested version; HEAD reads select their
version when admitted. Queue waiting has a deadline.

Each deferred scan declares nonzero broker memory. Its estimate includes request
storage, centroid state, required accumulator arrays, output destinations,
overlap, upload/protobuf copies, row/container overhead and bounded read-ahead.
Row sizes and batch row counts are bounded. Sample covering columns and filter
key-group caches have additional bounds. An individually impossible request
receives a permanent error; a full pending queue receives `ABORTED` with a retry
hint.

A node-shared quota bounds both pending count and retained request bytes across
DataShards. It charges protobuf storage plus serialized bytes and descriptor
allowance. A strict memory policy for `queue_build_index` prevents the broker's
idle-queue exception from admitting an oversized task. Other queues retain their
existing progress policy. A task larger than an absolute queue/global cap is
rejected explicitly.

A shared broker lease remains attached to the scan driver, result event, upload
actor and local upload request payload. It survives source-tablet shutdown and
is released after the final memory owner disappears. Late grants and queued
cancellation release once. Retrying an upload waits for retained payloads from
the preceding attempt to disappear before creating another payload copy.

Reshuffle uses an explicit assignment-only cluster mode and does not allocate
next-round accumulators. Recompute retains accumulators even with zero rounds.

SchemeShard bounds outstanding vector requests across builds by count and bytes,
with a per-build window and byte share. Admission overload produces delayed
retries with exponential backoff and jitter. Credit is released on terminal
replies, pipe retry and build-state cleanup.

## Defaults and memory-controller integration

| Setting | Default |
| --- | ---: |
| `IndexBuildLimitPercent` | 3% of node hard limit, clamped to activities budget |
| `BuildIndexPendingScansLimit` | 128 per node |
| `BuildIndexPendingBytesLimit` | 64 MiB, also capped at 1/8 of total index budget |
| `BuildIndexRequestBytesLimit` | 16 MiB serialized per request |
| `BuildIndexQueueTimeoutMs` | 60,000 |
| `BuildIndexScanMemoryLimit` | 1 GiB estimate per scan; broker caps also apply |
| `BuildIndexRowBytesLimit` | 8 MiB |
| `BuildIndexBatchRowsLimit` | 10,000 |
| `IndexBuildRequestCountLimit` | 128 per SchemeShard |
| `IndexBuildRequestBytesLimit` | 64 MiB per SchemeShard |
| `IndexBuildRequestsPerBuild` | 32 |

`IndexBuildLimitBytes` can override the percentage. The controller subtracts the
pending slice from both the active index queue and the global broker activities
limit, and clamps an explicit index queue limit to the derived allowance.
At a **16 GiB** hard limit with sufficient activities allowance, this gives
491.52 MiB total, 61.44 MiB pending and 430.08 MiB active reservations. Without
controller updates, the fallback is 512 MiB total / 64 MiB pending / 448 MiB active.
A lower dynamic limit stops further admission while existing owners drain.

The monitored quota is available under tablet counters, subgroup
`subsystem=build_index_admission`: `PendingScans`, `PendingBytes`, `RejectedScans`.
The controller publishes `Consumer/IndexBuild/Limit`.

These are conservative reservation estimates. For the upload measurement below,
a scan declares 118.77 MiB although the tested rows add about 28 MiB of live heap.
The estimate includes the configured worst-case row and metadata allowance;
production throughput may need tuning using representative data. They are not
a hard cap on total process RSS, storage caches or allocator-retained pages.
Transport/mailbox memory before the DataShard handles a request is outside the
pending quota; sender windows reduce that source of accumulation.

## Compatibility

The six requests add `AllowMemoryAdmission` (default false); replies add
`RetryAfterMs`. New SchemeShards opt in and understand delayed retries. Old
senders retain their existing scan path and receive no new overload hints.
New senders talking to old DataShards retain the existing reply behavior.
Consequently, full memory protection requires upgraded senders and DataShards;
legacy traffic is deliberately outside the new contract during rollout.

## Regression results

The production implementation compiled with `relwithdebinfo`. Both commands
below passed:

- **96 focused tests:** scan quota, K-means, resource broker, memory controller,
  deferred scans, pending saturation, oversized requests, delayed uploads,
  cancellation/replacement, timeout/late grant, explicit MVCC preservation,
  snapshot invalidation, row/group/sample bounds, sender windows and overload retry.
- **150 vector regression tests:** the existing DataShard scan suites and full
  `VectorIndexBuildTest` suite, including prefixed/overlapping builds, autodetect,
  rebuild, metering and error/cancellation paths. Some SchemeShard tests overlap
  with the focused run; these counts describe executions, not unique cases.

```bash
set -o pipefail
./ya make --build relwithdebinfo --keep-going -tA \
  ydb/core/base/ut ydb/core/tablet/ut ydb/core/memory_controller/ut \
  ydb/core/tx/datashard/build_index/ut ydb/core/tx/schemeshard/ut_index_build \
  -F 'TScanAdmission::*' -F 'NKMeans::*' -F 'TResourceBroker::*' \
  -F 'TMemoryController::*ResourceBroker*' \
  -F 'TMemoryController::IndexBuildBudgetIncludesPendingDescriptors' \
  -F 'TVectorIndexUploadMemory::*' \
  -F 'VectorIndexBuildTest::AdmissionOverloadRetriesAfterDelay' \
  -F 'VectorIndexBuildTest::SenderWindowIsSharedAcrossBuilds*' \
  -F 'VectorIndexBuildTest::CreateAndDrop' 2>&1 | tail -60

./ya make --build relwithdebinfo --keep-going -tA \
  ydb/core/tx/datashard/build_index/ut ydb/core/tx/schemeshard/ut_index_build \
  -F 'TTxDataShard*Scan::*' -F 'VectorIndexBuildTest::*' 2>&1 | tail -55
```

Builds ran in an isolated worktree at the same revision to avoid another local
build reading edited sources. Logs are retained in
`/tmp/ydb-56118-full-regressions.log` and `/tmp/ydb-56118-vector-regressions.log`.

## Post-fix profiling

The original two reproducers now accept `vector-upload-admission=true`.
Quantitative runs use `vector-upload-measure=true` with allocation sampling
disabled. Stack attribution uses separate runs with
`vector-upload-profile=true` and a 2 MiB sampling interval. Each numeric delta is
blocked minus baseline within one process, measured using TCMalloc live allocated
bytes and process RSS. MiB means 1,048,576 bytes.

The upload dataset and 8 MiB batches match the historical baseline. The fixture
uses the 448 MiB fallback active queue; it admits three 118.77 MiB reservations.
After unblocking, every submitted scan finishes with exact source/upload row
counts and the expected destination `COUNT(*)`.

| Submitted scans | Active while blocked | Live heap delta, MiB | Historical live delta, MiB | RSS delta, MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 28.018 | 27.986 | 3.160 |
| 4 | 3 | 83.871 | 111.011 | 1.500 |
| 8 | 3 | 83.963 | 224.931 | 0.742 |

Queued-centroid runs hold actual broker submissions before grant, read no source
rows, and use 1,024 centroids of 1,536 dimensions. The byte quota accepts 21
requests; further requests receive retryable overload. Accepted scans complete
after the hold is released.

| Submitted scans | Queued / rejected | Live heap delta, MiB | RSS delta, MiB |
| ---: | ---: | ---: | ---: |
| 1 | 1 / 0 | 0.351 | 0.727 |
| 32 | 21 / 11 | 0.916 | 0.949 |
| 128 | 21 / 107 | 1.096 | 3.012 |

The original 32-scan case added 430.269 MiB before admission. These matching
in-process runs share identical `TString` payload backing storage, as did the
historical case; quota charging conservatively counts each request independently.
For separately allocated payloads, use
`vector-centroid-distinct-payloads=true` to remove that sharing.


The independently allocated cases confirm the bound without shared backing
storage. The 21 waiting requests charge 63.812 MiB of the 64 MiB pending pool.
The 128-request case also passed through the standard `ya` test runner.

| Submitted scans | Queued / rejected | Charged pending bytes, MiB | Live heap delta, MiB | RSS delta, MiB |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 / 0 | 3.039 | 2.007 | 2.531 |
| 32 | 21 / 11 | 63.812 | 35.698 | 54.316 |
| 128 | 21 / 107 | 63.812 | 35.878 | 215.707 |

The direct-to-DataShard 128-request burst deliberately bypasses SchemeShard's
sender window. Requests rejected during processing leave reusable allocator
pages, explaining the larger RSS increase despite the same retained queue.
Sender credits and the pending quota cover different stages of this pipeline.

Separate sampled runs found no centroid-construction growth before grant. The
active-upload profile still attributes retained allocations to scan output
serialization and protobuf upload copies. Those categories disappear after scan
completion. Finished upload snapshots also contain destination-table data and
storage caches, so process totals do not return to the source-only baseline.
These deterministic tests preserve simultaneous lifetimes; they do not reproduce
production CPU parallelism, every transient peak or a kernel OOM kill.

Numeric snapshots are saved in
[memory_admission_measurements_56118.json](memory_admission_measurements_56118.json).
Raw measurements, heap samples, logs and symbolized analyses are retained in
`/tmp/ydb-56118-fix-memory/`. Scaling runs invoked the built test executable in
separate processes with explicit test contexts; those contexts are saved with
the cases. The command below runs the same reproducer through `ya`.

Example measurement (change the scan count to compare scaling):

```bash
set -o pipefail
./ya make --build relwithdebinfo -tA ydb/core/tx/datashard/build_index/ut \
  -F 'TVectorIndexUploadMemory::ReshuffleCentroidsBeforeAdmission' \
  --test-param vector-upload-admission=true \
  --test-param vector-upload-measure=true \
  --test-param vector-centroid-scans=128 \
  --test-param vector-centroid-clusters=1024 \
  --test-param vector-centroid-distinct-payloads=true 2>&1 | tail -40
```

Snapshots are written to the test's `testing_out_stuff` directory. Use
[profile_upload_memory.py](profile_upload_memory.py) with the exact executable
that generated a sample, the snapshot directory and an analysis output directory;
add `--prefix centroids-` for centroid runs.

# Local production group benchmark

Run one cell per process. This target uses real actor executor threads and real
PDisks on `TAllPDisksConfiguration::MkManyTmp` owned temporary regular files. It
does not accept a device path. Temporary files are removed with the configuration.
Set `TMPDIR` to an existing directory on the intended experiment filesystem.

The target is Linux-only, like the existing `ut_pdiskfit/pdiskfit` program.
Build with `./ya make --build relwithdebinfo ydb/core/blobstorage/benchmark_storage`.
For example, from the repository root:

```sh
TMPDIR=/path/to/experiment/tmp ydb/core/blobstorage/benchmark_storage/blobstorage-benchmark-storage \
  --species block-8-2 --workload get --size 1048576 --inflight 4 \
  --warmup 2 --seconds 8 --corpus 128 --output /path/to/experiment/cell
```

`--species` accepts `block-4-2` and `block-8-2`; `--workload` accepts `put`,
`get`, `restore`, `replication`, and `scrub`. `--missing 1` or `2` selects the
first one or two data parts, separately resolved for each blob's subgroup.
`--crc none|whole` selects the CRC policy for the custom driver. Healthy CRCNone
Put/Get use production `TStorageLoad` with `ContentType: Validated`; its exact
protobuf configuration and final HTML are saved. Other cells use production
TEvPut/TEvGet requests and compare every returned byte. `restore` issues
`MustRestoreFirst` once for each fixed corpus blob. `replication` starts paused
production replication after every VDisk reports full historical main ingress
and the exact surviving local parts for every corpus blob; `scrub` starts production scrub through a
minimal local admission service and injects identified extent read failures at
the PDisk request boundary. Other I/O is performed by the real PDisks. These
maintenance rows do not measure BSC admission policy or raw-device corruption.

Healthy LoadActor reads additionally compare every byte against the exact
repeated native blob-ID pattern used by `ContentType: Validated`. This is a
failing benchmark gate, including warmup and successful replies drained after
LoadActor completion. Non-OK statuses during warmup/measurement fail the run.
Non-OK statuses after cutoff are counted separately as censored tail/cleanup
errors: LoadActor's final hard GC may race its in-flight requests. Successful
responses with bad payload, ID or size always fail, including this tail.
The observer uses chunked `memcmp` with a bounded 6 KiB pattern;
it does not allocate a blob-sized copy. This validation CPU and observer work
are included in the measured process CPU and completion latency. A startup
self-check accepts a fragmented valid payload and rejects byte corruption at
the beginning, rope/pattern boundaries and tail, plus truncation. The benchmark
also rejects LoadActor completion before the measurement cutoff, even when its
final report uses the normal stop reason. Reply draining follows timing.
LoadActor's own duration timer is omitted: the benchmark parent forwards one
explicit stop after the cutoff, PMU/actor samples and end counter snapshot.
This avoids its timer being armed before its internal start timestamp, which
can label a normal duration expiry as an abort. A non-null successful final
report remains mandatory, as do the independent payload/error and early-stop
checks. The report must follow the recorded explicit stop. The configured
measurement duration and actual interval are in `result.json`; LoadActor's
optional duration field is absent. No fixed two-second shutdown tail is added.

Startup, seed, physical-part validation and warmup precede timing. An exact-byte
instrumentation preflight writes and reads one blob of the cell's logical size
and CRC mode, verifying one VGet and one part payload per main VDisk. This checks
the actual Huge/inline path and VPut size admission before timing. Its one blob
remains on a separate tablet for the duration of the process; probe size/tablet
and retained-blob count are explicit in `result.json`. It adds corresponding
Fresh or Huge allocation before the measured corpus is created.
The `empty` and `probe` counter snapshots bracket this allocation. Healthy Get
then keeps the configured corpus (128 blobs in the comparison matrix), plus
the retained same-size probe and LoadActor's one-byte sentinel at the maximum
step. Block, barrier and keep records add control metadata. Comparing `begin`
against `probe` includes the corpus, sentinel and control metadata; absolute
`empty` values retain the fixed overhead of eight or twelve VDisks.
Degraded serving verifies selected main records remain absent before timing;
custom seeding encrypts separately per blob ID before encoding. Maintenance completion uses
one-second VStatus/scrub checks and includes this polling overhead and up to one
second of completion granularity. Scrub requires two successful passes on each
affected disk. Replication and scrub completion additionally read and verify
every repaired main part before cutoff, with a final all-main check after
timing. A 300-second deadline makes failed maintenance runs finite.

MustRestoreFirst may complete with a handoff copy when a main is slow. Its
completion check reads every main and handoff VDisk for each corpus blob,
compares every successful copy with the exact per-ID encrypted and CRC-encoded
part, and requires `GetBlobState(layout, emptyFailedSet) == EBS_FULL`. Only
byte-verified, correctly identified, eligible physical copies enter the layout.
The full effective layout requires P distinct parts on P eligible distinct
disks; collecting P part types on fewer disks is insufficient. A bad redundant
copy also fails the check even when correct copies already form a full layout.
There is one complete scan before cutoff and another after cutoff; neither
waits for handoff copies to migrate to mains. All expected parts are prepared
before timing. Replication and scrub retain their strict main-part checks.

`restore_oracle_version: 2` identifies this completion policy. Its interval
includes the MustRestoreFirst request phase and full physical-layout readback,
including validation, observer work and trace construction. The request phase
wall/process CPU and readback phase wall/process CPU are also recorded
separately. The latter includes any concurrent background work. Completion
latencies still describe client Get requests, not physical verification reads.
`maintenance_verification_requests` and `maintenance_verification_payload_bytes`
count the timed direct checks; for restore they include all returned successful
payloads, including surviving and redundant copies. These reads are verification
traffic, not DSProxy repair reads. Interval VDisk/PDisk event totals include
them; their per-disk requests and successful payload bytes are explicit in the
layout trace. They must be shown separately when interpreting traffic and CPU
per recovered byte. Final readback and trace-file serialization are untimed.

Recovered physical bytes count each initially missing part type once per blob,
using its actual byte-verified payload size, regardless of destination or
duplicate main/handoff copies. `repaired_part_types` names this count for
restore; replication/scrub retain `repaired_main_parts`. The trace records all
destinations of every initially missing type, total verified main/handoff copy
counts and counts of recovered types with main, handoff or handoff-only copies.
`restore-layouts.json` and `result.json` contain both scans, all response statuses,
copy IDs, sizes, byte-validation results and effective layouts. Failures retain
completed-blob traces in `partial-result.json`. VDisk/PDisk writes are separate
interval totals and may include metadata or background traffic.

Only restore cells run four additional deterministic 4 KiB probes, on a separate
tablet, before seed/warmup/timing: full mains; a valid replacement on the first
handoff; two different lost types on the same handoff (P distinct types but P-1
effective replicas, rejected); and full valid mains plus one corrupt redundant
handoff copy (rejected). Expected rejection is a normal oracle result, not an
actor abort. These probes use CRCNone so the deliberately corrupt payload is
writable and reaches the byte validator. The four blobs and extra redundant
part remain until process cleanup; their tablet, CRC, logical and physical
payload bytes are recorded in `restore-oracle-preflight.json` and the result.
The existing cell-sized instrumentation probe remains separate. Non-restore
cells do not run these probes or use the full-layout oracle.

The replication fixture seeds only surviving payloads, then persists full
historical main-part ingress with no local bits through `TEvLocalSyncData` and
the real Skeleton recovery log/Hull receiver. Its actor ID is captured from the
normal recovery notification sent to VDisk Front. Without this historical
knowledge, parts that were never written create no replication tasks.
Before timing, every returned corpus ID, every main/handoff knowledge mask and
every surviving local mask is checked on every VDisk; direct reads also prove
the selected main payloads remain absent. This models in-process loss of known
payloads, not physical disk loss, restart, or a peer sync handshake. The local
fixture fragment uses a default sync cursor; replay after a restart could apply
that cursor to peer state, so no restart is part of these performance cells.
Replication itself starts only at the timed commence event and must write the
expected recovered bytes before the measurement ends.

`result.json` contains actual monotonic wall time, process CPU across all workers,
logical completions and bytes, raw completion latencies, actor elapsed activity
deltas when available, per-VDisk event payload bytes and per-PDisk submitted
chunk/log bytes. PDisk bytes exclude device-sector framing; injected failures
are separately counted. All matching completions in the interval are counted,
including starts during warmup. Outstanding requests at cutoff are reported
without a completion latency sample; CPU/event bytes are interval totals. The
cutoff and completion/latency updates share a mutex. Counter snapshots are saved every second and at window
boundaries. `failure.json` and stderr retain failures. `partial-result.json`
preserves available window metrics, PMU and LoadActor finish metadata on failure,
plus a failure snapshot of request/error/latency and disk counters. It is marked
failed/incomplete and is never an accepted measurement. Initialization and shutdown
are excluded. Results with fewer than 100 completions request a longer run; the
outer runner retains the initial sample and uses a separate 32-second attempt.

Use independent process repetitions, alternating species order, equal physical
CPU affinity and identical options. Capture source revision/diff, binary hash,
compiler configuration, CPU/NUMA/affinity and filesystem/device metadata in the
outer runner. Before worker creation, the binary opens inherited perf_event_open
counters for cycles, instructions, reference cycles, cache references/misses,
branches/misses, data-TLB read accesses/misses and instruction-TLB read misses.
These hardware events count user CPU only. Software task-clock, context-switch
and CPU-migration events include kernel execution; task-clock uses nanoseconds.
Counters are enabled around measurement and report their type, config, scope,
raw/scaled values and enabled/running time, or an explicit unsupported/open,
ioctl, read or scheduling reason. Short reads include the actual byte count.
After warmup, Block82 also records the initialized ISA-L encode dispatcher using
the existing read-only benchmark diagnostic; unknown stubs/targets are explicit.
Block42 records that this codec path is not used.
The four executor pools have 8/8/10/8 threads;
the ten-thread pool is I/O. Each logical domain has one VDisk and one PDisk; the
file-backed PDisk type is configured ROT and the Huge threshold is 64 KiB.
The benchmark fixes the VDisk maximum payload at 4 MiB for both species,
overriding the unit-test helper's 128 KiB default. This admits the matrix's
largest Block42 physical part (10 MiB logical / 4 plus CRC). Huge heap creation
requires at least two slots in every class; merely fitting the maximum payload
inside a chunk is insufficient. Before actor startup, the benchmark checks all
classes using the production layout builder, the formatter's 4,064-byte append
payload and the conservative requested 16 MiB chunk. With the helper's 64 KiB
milestone and overhead 8, the largest class is 4,246,880 bytes, including rounding
and the legacy-header allowance. At least three such slots fit the requested
chunk; the formatter's actual 18,726,912-byte user chunk holds four.
Each VDisk's effective limits, header policy, largest class and conservative
minimum slot count are saved. The actual preflight write/read also checks the
chosen part-size slot before measurements begin. The disk size remains 4 GiB;
the formatter requires more than 200 requested-size chunks per disk.
Monitoring uses local port 8088, so run cells sequentially. Logical domains share
the host and filesystem, and inter-host network bytes are zero. These are local
group measurements, not distributed-cluster throughput or isolated codec cost.

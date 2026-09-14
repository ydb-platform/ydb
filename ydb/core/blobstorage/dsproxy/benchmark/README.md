# Controlled DSProxy benchmark

This executable drives the production `TPutImpl` and `TGetImpl` against the same
persistent in-memory VDisk mock used by correctness tests. It complements the
real-group StorageLoad run. It does not model disk latency or claim that actor
virtual time is throughput.

Build from the repository root:

```sh
./ya make --build relwithdebinfo ydb/core/blobstorage/dsproxy/benchmark
```

Arguments are `species bytes operation mask iterations [crc]`:

- `species`: `42` or `82`.
- `operation`: `put`, `get`, or `restore` (`MustRestoreFirst`). Put is healthy.
- `mask`: `0`, `D`, `DD`, `DP`, `PP`, or a decimal subgroup-position bitmask.
  D/P refer to data/parity positions for the selected species. At most two
  positions may be missing. Missing mock contents return `NODATA`.
- `crc`: `none` (default) or `whole`.

Examples:

```sh
ydb/core/blobstorage/dsproxy/benchmark/dsproxy_bench 42 65536 put 0 100 none
ydb/core/blobstorage/dsproxy/benchmark/dsproxy_bench 82 1048576 get DD 100 none
ydb/core/blobstorage/dsproxy/benchmark/dsproxy_bench 82 4194304 restore DP 30 none
ydb/core/blobstorage/dsproxy/benchmark/dsproxy_bench 82 1048576 restore PP 100 whole
```

Run the complete controlled comparison with Python 3.9 or newer:

```sh
python3 ydb/core/blobstorage/dsproxy/benchmark/compare.py \
  ydb/core/blobstorage/dsproxy/benchmark/dsproxy_bench \
  /absolute/path/outside/checkout/dsproxy-comparison \
  --cpu 0 \
  --compiler-description 'Clang version from the build log' \
  --build-command './ya make --build relwithdebinfo ydb/core/blobstorage/dsproxy/benchmark' \
  --build-log /absolute/path/build.log
```

The runner uses 28 cells, both species, and ten independent processes per cell
and species (560 processes total):

| Cells | Sizes | Operations and missing parts | CRC |
|---|---|---|---|
| 8 | 64 KiB, 1 MiB, 4 MiB, 10 MiB | Healthy Put and Get | None |
| 16 | 1 MiB, 4 MiB | Get and restore, each with D/DD/DP/PP | None |
| 4 | 1 MiB | Healthy Put/Get, Get DD, restore DD | WholePart |

Each process performs `max(100, 128 MiB / blob_size)` measured iterations after
the three warmups. The child process is pinned to `--cpu` (default 0); the
runner fails early if that CPU is outside its permitted Linux affinity mask.
Species order alternates by repeat. No other benchmark/build should run during
measurement. Leave the source checkout and executable unchanged until it ends.

The output directory must be new; the runner refuses reuse or overwrite. It
saves a manifest with executable/runner SHA-256, Git commit, tracked and
untracked binary patches, CPU information, affinity, compiler/build/ISA
descriptions, and the exact matrix. Unknown compiler/kernel selection remains
explicitly unknown unless supplied through the description options. A source
snapshot and executable checksum are checked again at the end.

Every attempt has separate argv/metadata, stdout, stderr and exit information,
including failures and timeouts. Raw output is always kept. JSON is parsed only
after a zero exit and accepted only when species/cell parameters, validation
counts, timing denominators and operation-specific traffic checks agree.
Failed attempts remain recorded and yield a nonzero runner exit; there are no
automatic retries. `attempts.jsonl` journals results as they finish.
`summary.json` reports medians, median absolute deviations, and paired 82/42
ratios for CPU/wall time per byte and request/payload traffic. Zero-denominator
traffic ratios are omitted and counted explicitly. Sample/pair counts expose
incomplete cells. There is no performance threshold.

The JSON reports wall/process CPU nanoseconds per logical byte, operations,
VDisk request counts, payload read/write bytes and repair-write bytes. Payload
bytes include CRC trailers; protobuf framing and network overhead are excluded.
All issued mock I/O is drained, including requests whose replies become
unnecessary at client completion. This makes request/payload accounting exact
for the deterministic request order. Timed work includes the proxy algorithms,
in-memory mock payload copies, traffic accounting, and Put encryption/encoding.

Data, expected encoded parts and group monitors are prepared before timing.
Each operation resets the same blob's mock state before its timed region;
Get/restore preparation populates a complete encoded blob then removes the
selected parts. Three warmup operations are excluded. Every operation checks
status, exact full-blob bytes and all emitted part bytes; Put/restore also
checks a fresh Get of persisted mock contents. Byte comparisons, fixture
reset/population, result destruction and validation reads are outside timing.
Per-operation clock calls add a small fixed overhead shared by both species.

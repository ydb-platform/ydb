# Text: resident string columns on a scanned blob

Shows `PreferWasm` / `EnableWasmUdfResidentStringColumns` on the `Text` WASM UDF.

Unlike the Trie IP demo, the heavy argument is a **physical column of the
scanned table**, so `CollectWasmUdfStringColumns` marks it. The scan and the
UDF share one stage (`KqpReadRangesSource`).

Three tables, 1000 rows each:

| Table | Bytes / row | Table size |
|---|---|---|
| `text_200kb` | 200 KiB | ~200 MiB |
| `text_1mb` | 1 MiB | ~1 GiB |
| `text_2mb` | 2 MiB | ~2 GiB |

ASCII mix (~50% letters, ~20% digits, ~30% punctuation). Each row is a
byte-shifted slice of one tiled buffer, so the counters differ without
generating 3 GiB of unique payload in RAM.

Four query shapes (all `SUM(...) FROM table`, no `ORDER BY`). `SUM` compiles to
`Condense1` over the source rows, with the UDF inside its handlers — a form the
column resolver has to trace, or nothing is marked at all:

| Shape | SQL | Why |
|---|---|---|
| `probes` | 16 distinct `Text::byte_at(txt, k)` | 16 O(1) calls per value: host copies the blob 16 times, resident once |
| `letters` | `Text::count_letters(txt)` | one O(n) call; the copy is lost in the byte walk |
| `multi` | `count_letters + count_digits + count_upper` | three different O(n) exports (YQL does not collapse them) |
| `length` | `Text::text_length(txt)` | O(1) body, one call; the copy is the whole work |

## Cluster (from `ydbd/start.sh`)

```
-e grpc://localhost:2146 -d /Root/test
```

Tenant grpc is 31011; CLI goes to storage on 2146. Logs:

| File | Process |
|---|---|
| `/home/kulaad/ydbd/logs/storage_start.log` / `_err.log` | storage, grpc 2146 |
| `/home/kulaad/ydbd/logs/db_start.log` / `_err.log` | tenant `/Root/test`, grpc 31011 |

## Layout

| File | Role |
|---|---|
| `../main.cpp` | `count_letters` / `count_digits` / `count_upper` / `text_length` |
| `../manifest.json` | module `Text`, `string → int64` |
| `gen_demo_data.py` | `CREATE TABLE` + `bulk_upsert` |
| `gen_queries.py` | readable, evidence, load files; `--module TextNative --suffix _native` for native |
| `run_demo.sh` | A/B timing; `EVIDENCE=1` for the 1-row check |
| `demo_readable.sql` | first 5 rows of `text_200kb` with all counters |
| `demo_<table>_<shape>.sql` | full-scan load query |

## 1. SDK library and Text module

Text `required_libraries` is `["sdk"]`. Build both for Emscripten and upload
them (once per cluster):

```bash
cd /path/to/ydb
./ya make --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/sdk ydb/udfs/wasm/text
./ya make --build relwithdebinfo ydb/tests/functional/udf_store/upload_udf

UPLOAD=ydb/tests/functional/udf_store/upload_udf/upload_udf
$UPLOAD --endpoint grpc://localhost:2146 --database /Root/test \
    --kind library --library-name sdk --type WASM \
    --udf-file ydb/udfs/wasm/sdk/libwasm-sdk.so
$UPLOAD --endpoint grpc://localhost:2146 --database /Root/test \
    --type WASM --manifest ydb/udfs/wasm/text/manifest.json \
    --udf-file ydb/udfs/wasm/text/libwasm-text.so
```

Exact `.so` names follow `ya make` output (`ls ydb/udfs/wasm/{sdk,text}/*.so`).
After upload, restart **both** nodes together so `TUdfStoreInitializer` picks
the new modules up. For evidence, export `YDB_WASM_STRING_DEBUG=1` before
`/home/kulaad/ydbd/restart_cluster.sh`.

Smoke (`query.sql`, expected `letters=3, digits=3, upper=1, len=7`):

```bash
ydb -e grpc://localhost:2146 -d /Root/test sql -f ydb/udfs/wasm/text/query.sql
```

## 2. Data

```bash
cd ydb/udfs/wasm/text/demo
python3 gen_demo_data.py --dry-run
python3 gen_demo_data.py \
    --endpoint grpc://localhost:2146 --database /Root/test \
    --rows 1000
# or one table first:
python3 gen_demo_data.py --only text_200kb --rows 1000
```

`--only` is a comma-separated list of table names. `--batch-bytes` (default
8 MiB) caps one `bulk_upsert`.

## 3. Queries

```bash
python3 gen_queries.py --readable-rows 5 --readable-table text_200kb
```

**Readable** (`demo_readable.sql`) — first 5 rows of `text_200kb`: `head`,
`letters`, `digits`, `upper`, `len`. Row 1 matches the Python reference
(`letters=102540, digits=41019, upper=20361, len=204800`).

**Load** — one file per `(table, shape)`, `SUM` without `ORDER BY`.

## 4. Evidence that residency actually engaged

Per-query counters live in the **node log**, component `KQP_COMPUTE` at INFO.
No restart needed:

```bash
curl "http://localhost:31012/actors/logger?c=535&p=6"   # tenant mon-port
```

Run a query and read the last summary line:

```bash
ydb -e grpc://localhost:2146 -d /Root/test sql -f demo_text_1mb_probes.sql
grep "Wasm resident string columns" /home/kulaad/ydbd/logs/db_start.log | tail -1
```

PreferWasm **on** (`text_1mb probes`, 1000 rows × 16 calls per row):

```
Wasm resident string columns columns=txt copiedIntoCompartment=0 \
    materializedInWasm=1000 residentReused=16000 task=1 txId=...
```

PreferWasm **off**:

```
Wasm resident string columns columns= copiedIntoCompartment=16000 \
    materializedInWasm=0 residentReused=0 task=1 txId=...
```

| Field | Meaning |
|---|---|
| `columns` | what the compiler marked for this task |
| `materializedInWasm` | column values written into linear memory (one per row) |
| `residentReused` | UDF args that took those bytes as is |
| `copiedIntoCompartment` | args still copied on every call |

`columns=` empty together with a non-zero `copiedIntoCompartment` means the
resolver did not mark the column — that is how **every** `SUM(...)` shape here
behaved until fold handlers (`Condense1` and friends) were traced, so all A/B
numbers taken before that compared baseline with baseline.

Never expected: `wasm UDF string column was materialized without a query
compartment`.

Per-row tracing is a separate, much louder switch: start the tenant node with
`YDB_WASM_STRING_DEBUG=1` and every value prints `[WasmString] Register` /
`TryFree` to `logs/db_start_err.log`. Useful only on single-row queries:

```bash
EVIDENCE=1 ./run_demo.sh
```

## 5. A/B timing

```bash
WARMUP=1 RUNS=5 ./run_demo.sh
# subset:
TABLES=text_200kb SHAPES="probes letters multi length" WARMUP=1 RUNS=5 ./run_demo.sh
```

Check the log line from section 4 first: an A/B where `materializedInWasm` is 0
in both modes measures nothing.

Defaults: `-e grpc://localhost:2146 -d /Root/test`. One warmup per
`(table, shape, mode)` is discarded (compartment + compile). Timed runs are
interleaved `true`/`false`. Negative delta = resident is faster. Measured
runs that still compiled from scratch are flagged (`from_cache != true`).

### Measured on this cluster

`start.sh disk`, `-e grpc://localhost:2146 -d /Root/test`. 1000 rows, median of
3 runs, warmup excluded, `from_cache: true` on measured runs, no
FallbackNoCompartment. Every row of the table was checked against the log line
from section 4: `materializedInWasm=1000` with resident on, `0` with it off.

| table | shape | calls / value | res wall | res cpu | host wall | host cpu | Δ cpu |
|---|---|---|---|---|---|---|---|
| text_1mb | probes | 16 | 685 ms | 90 ms | 1454 ms | 1178 ms | **−92.4%** |
| text_1mb | length | 1 | 740 ms | 51 ms | 592 ms | 129 ms | **−60.7%** |
| text_200kb | probes | 16 | 356 ms | 64 ms | 354 ms | 128 ms | **−50.4%** |
| text_200kb | multi | 3 | 14205 ms | 13961 ms | 14150 ms | 13873 ms | +0.6% |
| text_1mb | letters | 1 | 26585 ms | 26189 ms | 26776 ms | 26383 ms | −0.7% |
| text_200kb | length | 1 | 342 ms | 38 ms | 303 ms | 29 ms | +30.4% |

The pattern is not "many calls win, one call loses" — it is how much of the CPU
the UDF body itself eats:

- `probes` and `length` on 1 MiB: the body is O(1), so argument preparation *is*
  the query. Host copies 16 GiB (`probes`) or 1 GiB (`length`) into linear
  memory; resident copies 1 GiB once, at materialization.
- `letters` / `multi`: the body walks every byte in WASM (13–26 s of CPU), so one
  saved copy per value is invisible.
- `length` on 200 KiB is the one loss: a single call on a value small enough that
  the registry insert under a lock costs more than the copy it removes. The
  microbenchmark break-even (`wasm/benchmark`) sits in the same region.

Wall time on the short queries is dominated by the scan and by fixed request
overhead, so read Δ cpu, not Δ wall, unless the query runs for seconds.

Use the **readable** query for the slide, `probes` for the load numbers, and the
section 4 log line to show that `txt` actually went resident.

## 6. Native baseline (`TextNative`)

The native module lives in [ydb/udfs/native/text](../../native/text/). It
mirrors the WASM `Text` exports but runs as an in-process `.so` loaded via
`--udfs-dir` (see that directory's README). PreferWasm does not apply.

Setup:

```bash
./ya make --build relwithdebinfo ydb/udfs/native/text
mkdir -p /home/kulaad/ydbd/udfs
ln -sf $(pwd)/ydb/udfs/native/text/libtext_native_udf.so /home/kulaad/ydbd/udfs/
# restart both nodes with --udfs-dir /home/kulaad/ydbd/udfs (see restart_cluster.sh)

cd ydb/udfs/wasm/text/demo
python3 gen_queries.py --module TextNative --suffix _native
NATIVE=1 TABLES=text_1mb SHAPES="probes length" RUNS=3 ./run_demo.sh
```

Output adds `nat_ms` / `nat_cpu_us` columns alongside resident (`res_*`) and
host-copy WASM (`host_*`).

### Measured on this cluster (with native)

Same cluster as section 5, plus `TextNative` in `--udfs-dir`. 1000 rows,
median of 3 runs, warmup excluded, `from_cache: true`.

| table | shape | res cpu | host cpu | native cpu |
|---|---|---|---|---|
| text_1mb | probes | 80 ms | 1114 ms | **13 ms** |
| text_1mb | length | 10 ms | 141 ms | **7 ms** |

Native avoids WASM sandbox overhead and host→guest copies entirely. Resident
WASM closes most of the gap vs host-copy WASM on copy-bound shapes (`probes`,
`length`), but native remains faster when the UDF body is O(1). On O(n) shapes
(`letters`, `multi`) expect all three to converge — the byte walk dominates.

## Knobs

| Env / flag | Default | Meaning |
|---|---|---|
| `--rows` | 1000 | rows per table |
| `--only` | all three | comma-separated table names to load |
| `--batch-bytes` | 8 MiB | `bulk_upsert` payload cap |
| `TABLES` | `text_1mb` | which tables to time |
| `SHAPES` | `probes letters` | which load queries |
| `RUNS` | 5 | timed iterations per (table, shape, mode) |
| `WARMUP` | 1 | discarded runs per mode (compartment + compile) |
| `NATIVE` | 0 | also time `TextNative` load queries (`*_native.sql`) |
| `NATIVE_SUFFIX` | `_native` | filename suffix for native load SQL |
| `ENDPOINT` / `DB` | `grpc://localhost:2146` / `/Root/test` | from `ydbd/start.sh` |
| `YDBD_ROOT` / `YDB_LOG` | `/home/kulaad/ydbd` / `$YDBD_ROOT/logs/db_start_err.log` | tenant stderr |

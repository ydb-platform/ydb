# TextNative: native baseline for the Text WASM demo

Native YQL UDF module with the same five functions as the WASM `Text` module
([../wasm/text/main.cpp](../wasm/text/main.cpp)). Use it as a third benchmark
point: resident WASM, host-copy WASM, and in-process native code.

| Function | Signature | Semantics |
|---|---|---|
| `count_letters` | `String → Int64` | ASCII `[A-Za-z]` |
| `count_digits` | `String → Int64` | ASCII `[0-9]` |
| `count_upper` | `String → Int64` | ASCII `[A-Z]` |
| `text_length` | `String → Int64` | byte length |
| `byte_at` | `String, Int64 → Int64` | byte at index; out of range → `0` |

## Build

```bash
cd /path/to/ydb
./ya make --build relwithdebinfo ydb/udfs/native/text
ls ydb/udfs/native/text/libtext_native_udf.so
```

The build runs `udf_probe` at link time; a broken `.so` fails here.

## Load into ydbd (`--udfs-dir`)

ydbd scans `--udfs-dir` recursively for `lib*.so` (not `libtest_*.so`) and
`dlopen`s each library at startup.

```bash
mkdir -p /home/kulaad/ydbd/udfs
ln -sf /path/to/ydb/ydb/udfs/native/text/libtext_native_udf.so /home/kulaad/ydbd/udfs/
```

Add to **both** storage and tenant `ydbd server` invocations:

```
--udfs-dir /home/kulaad/ydbd/udfs
```

On startup the log should contain:

```
UDF directory /home/kulaad/ydbd/udfs contains 1 dynamic UDFs.
```

## Smoke test

```bash
ydb -e grpc://localhost:2146 -d /Root/test sql -f ydb/udfs/native/text/query.sql
```

Expected: `letters=3, digits=3, upper=1, len=7, b0=65, b1=98`.

## Demo integration

Generate native load queries and run the three-way benchmark:

```bash
cd ydb/udfs/wasm/text/demo
python3 gen_queries.py --module TextNative --suffix _native
NATIVE=1 TABLES=text_1mb SHAPES="probes length" RUNS=3 ./run_demo.sh
```

Columns: `res_*` (PreferWasm on), `host_*` (PreferWasm off), `nat_*`
(TextNative, no WASM resident path).

## Alternative: UDF Store `NATIVE_UNSAFE`

The same `libtext_native_udf.so` can be uploaded via `upload_udf
--type NATIVE_UNSAFE` if `enable_unsafe_native_udf: true` and
`unsafe_native_udf_dir` are set in cluster config. No manifest is required.
This demo uses `--udfs-dir` instead.

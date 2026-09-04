# TrieNative: native baseline for the Trie WASM demo

Native YQL UDF module with the same two free functions as the WASM `Trie`
module ([../wasm/trie/main.cpp](../wasm/trie/main.cpp)). Use it as an
in-process benchmark point next to resident / host-copy WASM.

`LookupCached` (WASM TypeConfig / object framework) is **not** mirrored here.

| Function | Signature | Semantics |
|---|---|---|
| `Lookup` | `String, String → Int64` | payload offset; miss → `-1` |
| `LookupWithString` | `String, String → Optional\<String\>` | payload bytes; miss → `NULL` |

Logic comes from the shared header-only
[`binary_trie.h`](../wasm/trie/binary_trie.h).

## Build

```bash
cd /path/to/ydb
./ya make --build relwithdebinfo ydb/udfs/native/trie
ls ydb/udfs/native/trie/libtrie_native_udf.so
```

The build runs `udf_probe` at link time; a broken `.so` fails here.

## Load into ydbd (`--udfs-dir`)

ydbd scans `--udfs-dir` recursively for `lib*.so` (not `libtest_*.so`) and
`dlopen`s each library at startup.

```bash
mkdir -p /home/kulaad/ydbd/udfs
ln -sf /path/to/ydb/ydb/udfs/native/trie/libtrie_native_udf.so /home/kulaad/ydbd/udfs/
```

`--udfs-dir` is already on both nodes in `start.sh` / `restart_cluster.sh`.
Restart after adding the symlink. The log should mention more than one dynamic
UDF if `TextNative` is also present.

## Smoke test

```bash
ydb -e grpc://localhost:2146 -d /Root/test sql -f ydb/udfs/native/trie/query.sql
```

Expected: `hit=10`, `miss=-1`, `label` non-null, `nores=NULL`.

## Demo integration

```bash
cd ydb/udfs/wasm/trie/demo
python3 gen_queries.py --module TrieNative --suffix _native
DICT_FROM=1 DICT_TO=1 NATIVE=1 CONST=1 ./run_demo.sh
```

PreferWasm / `ResidentConstArgs` do not apply to native: there is no WASM
compartment.

## Alternative: UDF Store `NATIVE_UNSAFE`

The same `.so` can be uploaded via `upload_udf --type NATIVE_UNSAFE` if
`enable_unsafe_native_udf: true` is set. This demo uses `--udfs-dir` instead.

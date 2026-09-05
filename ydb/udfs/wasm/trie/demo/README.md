# Trie + IP ACL: bridge pin demo

Shows **WASM Bridge** reuse of a precomputed dictionary blob:

- `Trie::LookupPinned` / `LookupWithStringPinned` use `calling_convention: "bridge"`.
- Host `RegisterOrReuse` keeps a stable handle for the same `$dict` across rows —
  boxed values by object pointer, strings by refcounted buffer.
- `BridgeEnsureString` lazily copies `$dict` into the compartment resident cache
  **once**; later rows get the same offset back (no per-row `CopyIntoCompartment`).
  The cache outlives the node, so `BridgeRef` is not needed for this to hold.
- `Trie::LookupCachedBlob` shows the general form: guest state (here a parsed
  header) built once and kept in the per-node user-data slot, freed by the guest
  when the host reports the node as released.

Legacy `Trie::Lookup` / `LookupWithString` (`unversioned_value`) still copy the
blob on every row — use them only for ABI smoke tests.

Two tables:

| Table | Rows | Content |
|---|---|---|
| `ip_addr` | 10 000 | IPv4 keys only (`id`, `ip`, 4-byte `addr`) |
| `ip_dict` | 10 | Trie0001 blobs of **1, 2, …, 10 MiB** (`id`, `size_mb`, `acl`) |

Load query shape:

```sql
$dict = SELECT Unwrap(MIN(acl)) FROM ip_dict WHERE id = N;
SELECT SUM(Trie::LookupPinned(addr, $dict)) AS checksum FROM ip_addr;
```

## Cluster (from `ydbd/start.sh`)

After `/home/kulaad/ydbd/start.sh disk` the script prints:

```
-e grpc://localhost:2146 -d /Root/test
```

That is what every command below uses (`ENDPOINT` / `DB` defaults match it).
The tenant process itself listens on grpc 31011; the CLI goes to the storage
node on 2146. Logs:

| File | Process |
|---|---|
| `/home/kulaad/ydbd/logs/storage_start.log` / `_err.log` | storage, grpc 2146 |
| `/home/kulaad/ydbd/logs/db_start.log` / `_err.log` | tenant `/Root/test`, grpc 31011 |

## Layout

| File | Role |
|---|---|
| `trie_blob.py` | Trie0001 builder + Python lookup (mirrors `binary_trie.h`) |
| `gen_demo_data.py` | `CREATE TABLE ip_addr` / `ip_dict`, `bulk_upsert` |
| `gen_queries.py` | Readable, evidence, and `demo_load_01.sql` … `demo_load_10.sql` |
| `run_demo.sh` | timed `LookupPinned` load queries; `EVIDENCE=1` for the 1-row check |
| `demo_readable.sql` | First 10 addresses × dictionary 1 (`LookupWithStringPinned`) |
| `demo_load_NN.sql` | Full `ip_addr` scan against dictionary `NN` via `LookupPinned` |

## 1. SDK library and Trie module

Trie `required_libraries` is `["sdk"]`. Build both for Emscripten and upload
them (once per cluster):

```bash
cd /path/to/ydb
./ya make --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/sdk ydb/udfs/wasm/trie
./ya make --build relwithdebinfo ydb/tests/functional/udf_store/upload_udf

UPLOAD=ydb/tests/functional/udf_store/upload_udf/upload_udf
$UPLOAD --endpoint grpc://localhost:2146 --database /Root/test \
    --kind library --library-name sdk --type WASM \
    --udf-file ydb/udfs/wasm/sdk/libudfs-wasm-sdk.so
$UPLOAD --endpoint grpc://localhost:2146 --database /Root/test \
    --type WASM --manifest ydb/udfs/wasm/trie/manifest.json \
    --udf-file ydb/udfs/wasm/trie/libudfs-wasm-trie.so
```

Exact `.so` names follow `ya make` output (`ls ydb/udfs/wasm/{sdk,trie}/*.so`).
After upload, restart **both** nodes together so `TUdfStoreInitializer` picks
the new modules up (`/home/kulaad/ydbd/restart_cluster.sh`).

Smoke (unversioned fixture from `query.sql`, expected `hit = 10`):

```bash
ydb -e grpc://localhost:2146 -d /Root/test sql -s \
  'SELECT Trie::Lookup(String::HexDecode("80000000000000000000000000000000"),
         String::HexDecode("5472696530303031200000000100000000000000100000000000000000000080000000000a00000000000000")) AS hit;'
```

## 2. Data

```bash
cd ydb/udfs/wasm/trie/demo
python3 trie_blob.py --self-test
python3 gen_demo_data.py --dry-run
python3 gen_demo_data.py \
  --endpoint grpc://localhost:2146 --database /Root/test
python3 gen_queries.py --dicts 10
```

## 3. Timing

```bash
./run_demo.sh
# optional: EVIDENCE=1 ./run_demo.sh
# native baseline: NATIVE=1 ./run_demo.sh  (TrieNative via --udfs-dir)
```

Expect WASM `LookupPinned` wall time much closer to native than the old
per-row `Lookup` copy path (especially as dictionary MiB grows).

Measured on the local `ydbd` cluster (10 000 addresses per query, warmup excluded):

| dict MiB | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| wall ms | 263 | 396 | 438 | 466 | 496 | 461 | 470 | 478 | 471 | 412 |

Median 463 ms wall / 216 ms server CPU. Wall time stays flat in dictionary size
because the blob is copied into the compartment once, not once per row.

# Trie + IP ACL: bridge pin demo

Shows **WASM Bridge** reuse of a precomputed dictionary blob:

- `Trie::Lookup` / `LookupWithString` use `calling_convention: "bridge"`.
- Host `RegisterOrReuse` + `BridgeEnsureString` pin `$dict` once per distinct
  value (`BridgeRef` not required).
- `Trie::LookupCachedBlob` — guest state via `BridgeGetOrBuild`.
- `Trie::LookupDict` — host `Dict` behind the bridge.

Two tables:

| Table | Rows | Content |
|---|---|---|
| `ip_addr` | 10 000 | IPv4 keys (`id`, `ip`, `addr`) |
| `ip_dict` | 3 | Trie0001 blobs of **1, 2, 3 MiB** |

Load shape (built in `run_demo.py`):

```sql
$dict = SELECT Unwrap(MIN(acl)) FROM ip_dict WHERE id = N;
SELECT SUM(Trie::Lookup(addr, $dict)) AS checksum FROM ip_addr;
```

## Layout

| File | Role |
|---|---|
| `trie_blob.py` | Trie0001 builder + Python lookup |
| `gen_demo_data.py` | `CREATE TABLE` + `bulk_upsert` |
| `run_demo.py` | build SQL with substitutions and run |

## Setup

```bash
cd /path/to/ydb
./ya make --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/sdk ydb/udfs/wasm/trie
# upload sdk + Trie, restart both nodes

ydb -e grpc://localhost:2146 -d /Root/test sql -f ydb/udfs/wasm/trie/query.sql
```

## Data

```bash
cd ydb/udfs/wasm/trie/demo
python3 trie_blob.py --self-test
python3 gen_demo_data.py --dry-run
python3 gen_demo_data.py --endpoint grpc://localhost:2146 --database /Root/test
```

## Run

```bash
python3 run_demo.py --readable
python3 run_demo.py --evidence
python3 run_demo.py --dict-from 1 --dict-to 3
python3 run_demo.py --native --dict-from 1 --dict-to 1
```

Defaults: `-e grpc://localhost:2146 -d /Root/test`. Env overrides:
`ENDPOINT`, `DB`, `ADDR_TABLE`, `DICT_TABLE`, `DICT_FROM`, `DICT_TO`,
`WARMUP`, `NATIVE=1`, `YDB`.

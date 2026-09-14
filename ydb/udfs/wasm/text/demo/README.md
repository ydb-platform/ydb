# Text: bridge pin of a scanned string column

Shows **WASM Bridge** (`calling_convention: "bridge"`) on the `Text` UDF over a
**physical column** of the scanned table.

`RegisterOrReuse` + `BridgeEnsureString` pin each distinct `txt` value into
compartment linear memory once; later calls on the same value (e.g. 16×
`byte_at`) reuse the offset — no per-call `CopyIntoCompartment`.

Two tables, 1000 rows each:

| Table | Bytes / row | Table size |
|---|---|---|
| `text_200kb` | 200 KiB | ~200 MiB |
| `text_1mb` | 1 MiB | ~1 GiB |

Four query shapes (built in `run_demo.py`, no checked-in `.sql`):

| Shape | SQL | Why |
|---|---|---|
| `probes` | 16× `Text::byte_at(txt, k)` | pin once, reuse 15× |
| `letters` | `Text::count_letters(txt)` | O(n) body |
| `multi` | letters + digits + upper | three O(n) exports |
| `length` | `Text::text_length(txt)` | O(1) body |

## Layout

| File | Role |
|---|---|
| `../main.cpp` / `../manifest.json` | bridge Text module |
| `gen_demo_data.py` | `CREATE TABLE` + `bulk_upsert` |
| `run_demo.py` | build SQL with substitutions and run (readable / evidence / bench) |

## Setup

```bash
cd /path/to/ydb
./ya make --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/sdk ydb/udfs/wasm/text
./ya make --build relwithdebinfo ydb/tests/functional/udf_store/upload_udf
# upload sdk + Text, restart both nodes (see wasm-udf-deploy skill)

ydb -e grpc://localhost:2146 -d /Root/test sql -f ydb/udfs/wasm/text/query.sql
```

## Data

```bash
cd ydb/udfs/wasm/text/demo
python3 gen_demo_data.py --dry-run
python3 gen_demo_data.py --endpoint grpc://localhost:2146 --database /Root/test --rows 1000
```

## Run

```bash
python3 run_demo.py --readable
python3 run_demo.py --evidence
python3 run_demo.py --tables text_1mb --shapes "probes letters" --runs 5
python3 run_demo.py --native --tables text_1mb --shapes "probes length" --runs 3
```

Defaults: `-e grpc://localhost:2146 -d /Root/test`. Env overrides:
`ENDPOINT`, `DB`, `TABLES`, `SHAPES`, `RUNS`, `WARMUP`, `NATIVE=1`, `YDB`.

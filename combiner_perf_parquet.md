# `combiner_perf` Parquet mode

## Task

Add a new `-t parquet` mode to `ydb/core/kqp/tools/combiner_perf/bin` for quick isolated performance, profiling, correctness, crash, and sanitizer checks of `DqHashAggregate` with Arrow block input sourced from a Parquet file (initially `hits.parquet`, the ClickBench dataset).

Requirements were:

- Select input columns and derive their MKQL simple data-slot types from the Parquet/Arrow schema.
- Select one or more key columns.
- Support synthesized `sum:column_name` and per-key `count` aggregations; count is initialized to 1 and updated with `AggrAdd(1, state)`.
- Optionally limit the number of input tuples.
- Pre-read the selected data into RAM and wrap Arrow blocks in `TUnboxedValue`s before timing to reduce runtime noise.
- Run every measurement attempt in a forked process.
- Do not include a timed WideCombiner reference comparison: WideCombiner cannot consume Arrow blocks directly, and conversion would pollute measurements.
- Provide an untimed correctness check by any suitable slower method.
- Make all new CLI parameters valid only for `-t parquet`, reporting an error otherwise.
- Keep this change limited to `DqHashAggregate` block mode. Adding `DqHashCombine` block mode is a possible follow-up.

## Commit

- Commit: `63c67c8bf45fa212afcf0edf4751a6108ceeb3c9` (`63c67c8bf45`)
- Subject: `Implement the -t parquet mode in combiner_perf for running perf tests over static datasets`
- Branch at commit time: `combiner-perf-clickbench`

## Implemented changes

- Added `ydb/core/kqp/tools/combiner_perf/parquet.cpp` and `parquet.h`.
- Registered `parquet.cpp` and `contrib/libs/apache/arrow` in the tool library's `ya.make`.
- Added `ETestType::Parquet` and LLVM/spilling template dispatch in `bin/main.cpp`.
- Added Parquet configuration fields to `TRunParams`.
- Added Parquet-specific console/JSON metrics instead of synthetic-generator fields.

New CLI options:

- `--parquet-file PATH` — required input file.
- `--parquet-row-limit ROWS` — maximum rows to preload; zero or omission means the whole file.
- `--parquet-columns NAME,...` — required selected columns, preserving input order.
- `--parquet-keys NAME,...` — required key columns; every key must be in `--parquet-columns`.
- `--parquet-aggregations AGG,...` — required list containing `sum:column_name` and/or `count`; sum columns must be selected.

The mode accepts the existing `--block-size`, `--run-count`, `--num-attempts`, `--no-verify`, `--llvm`, and `--spilling` controls. It accepts only `--mode=all` and `--mode=graph`; no timed reference-only/generator-only path exists. Parquet-specific options are rejected for every other `-t` mode.

### Input and graph construction

- Opens the file with vendored Arrow/Parquet (`contrib/libs/apache/arrow`, including `parquet/arrow/reader.h`).
- Resolves requested columns against the Arrow schema, rejects missing/duplicate names, and maps supported primitive Arrow types to MKQL data slots.
- Handles integer, floating-point, Boolean, UTF-8/binary string, date32/date64, and timestamp physical types. Arrays are cast to the physical Arrow representation expected by MKQL blocks where necessary (for example Boolean to `uint8`). Unsupported complex types fail with an explicit error.
- Uses the requested block size as the Parquet record-batch size, stops precisely at the row limit, retains all selected arrays in RAM, and reports inferred types.
- Builds a block-wide stream type containing one `TBlockType::Many` per selected column plus the scalar `Uint64` block-length column.
- Synthesizes key extraction, initialization, update, and finalization lambdas and builds `DqHashAggregate` through `TKqpProgramBuilder`.
- Wraps retained arrays and block lengths into `TUnboxedValue` Arrow blocks before measurements. `--run-count` replays the prebuilt blocks without rereading the file.

### Measurement isolation

- The dataset, graph, and prebuilt block stream are prepared in the parent.
- Every `--num-attempts` run uses `RunForked`, so each child receives the same pristine graph/input through fork copy-on-write.
- Timing covers graph consumption only. Reference timing remains zero, and the best runtime/max RSS are merged with the existing metrics machinery.

### Correctness checking

- Unless `--no-verify` is specified, a separate untimed fork consumes the block `DqHashAggregate` result.
- The retained Arrow input is scalarized lazily into a separate scalar WideCombiner graph.
- WideCombiner uses memory limit `0`, which selects full aggregation mode. Any nonzero value selects pre-aggregation and can emit partial duplicate groups.
- Results are compared by encoded composite keys and aggregate values. Integer results are exact; floating-point comparisons use a small relative tolerance. Empty strings and optional/null values are handled.

## Example

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t parquet \
  --parquet-file hits.parquet \
  --parquet-row-limit 20000 \
  --parquet-columns CounterID,RegionID,UserID \
  --parquet-keys CounterID,RegionID \
  --parquet-aggregations sum:UserID,count \
  --block-size 128 \
  --run-count 2
```

## Validation performed

- Built from the checkout root:

  ```bash
  ./ya make --build relwithdebinfo ydb/core/kqp/tools/combiner_perf/bin
  ```

- Verified numeric keys and `sum` + `count` against `hits.parquet`.
- Verified UTF-8 keys, including empty strings, with `count`.
- Verified multi-column keys, multiple aggregations, `--block-size 128`, and `--run-count 2`.
- Verified two measurement attempts execute successfully in separate forked processes.
- Smoke-tested `--llvm` and `--spilling`.
- Confirmed Parquet-specific options produce an error with another test mode.
- `git diff --check` passed before commit.

## Follow-up scope

- Add a block-input `DqHashCombine`/pre-aggregation target later, likely reusing the Parquet loading, type mapping, stream, and graph-lambda construction in `parquet.cpp`.
- A future performance reference target can replace or supplement the correctness-only scalar WideCombiner path.

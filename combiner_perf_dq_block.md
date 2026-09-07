# `combiner_perf` DQ block mode

This mode was originally called `parquet`. It is now named `dq-block` because Parquet is only one possible source of block data. It currently supports Parquet files and a shuffled `Uint32` sample generator.

## Task

Add a new `-t dq-block` mode to `ydb/core/kqp/tools/combiner_perf/bin` for quick isolated performance, profiling, correctness, crash, and sanitizer checks of `DqHashAggregate` with Arrow block input sourced from a Parquet file (initially `hits.parquet`, the ClickBench dataset).

Requirements were:

- Select input columns and derive their MKQL simple data-slot types from the Parquet/Arrow schema.
- Select one or more key columns.
- Support synthesized `sum:column_name` and per-key `count` aggregations; count is initialized to 1 and updated with `AggrAdd(1, state)`.
- Optionally limit the number of input tuples.
- Pre-read the selected data into RAM and wrap Arrow blocks in `TUnboxedValue`s before timing to reduce runtime noise.
- Run every measurement attempt in a forked process.
- Do not include a timed WideCombiner reference comparison: WideCombiner cannot consume Arrow blocks directly, and conversion would pollute measurements.
- Provide an untimed correctness check by any suitable slower method.
- Make all new CLI parameters valid only for `-t dq-block`, reporting an error otherwise.
- Keep this change limited to `DqHashAggregate` block mode. Adding `DqHashCombine` block mode is a possible follow-up.
- Support non-file sources, beginning with a reproducibly shuffled `Uint32` range.

## Commits

- `3723137c0b2e8457efeb3cbbfb41605d40699f5b` (`3723137c0b2`) — `Implement the -t parquet mode in combiner_perf for running perf tests over static datasets`
- `f5357d6d96ac24a891acf29c4020907cf48c260b` (`f5357d6d96a`) — `explicit ast for combiner_perf lambdas in parquet mode`
- Current branch: `combiner-perf-ast`

These are the commit hashes after rebasing onto `e0b13ea3ee69b9496ecbdade5d3aa24a1d7a8fc9` (`e0b13ea3ee6`, `origin/main`).

## Implemented changes

- Added `ydb/core/kqp/tools/combiner_perf/dq_block.cpp` and `dq_block.h`.
- Registered `dq_block.cpp` and `contrib/libs/apache/arrow` in the tool library's `ya.make`.
- Added `ETestType::DqBlock` and LLVM/spilling template dispatch in `bin/main.cpp`.
- Added DQ block configuration fields to `TRunParams`.
- Added DQ-block-specific console/JSON metrics instead of synthetic-generator fields.
- Renamed the test entry point to `RunTestDqBlock`, the in-memory data/stream types to `TDqBlock*`, and the JSON fields from `parquet*` to `dqBlock*`.

New CLI options:

- `--dq-block-file PATH` — input file, currently in Parquet format; mutually exclusive with `--dq-block-generator`.
- `--dq-block-generator shuffle[:SEED]` — generate a shuffled `Uint32` column named `i`; the optional numeric seed makes the shuffle reproducible. Without it, the existing `--rand-seed` value is used (and defaults to the current time).
- `--dq-block-row-limit ROWS` — maximum rows to preload from a file; required generated row count for the shuffle source.
- `--dq-block-columns NAME,...` — selected file columns, preserving input order. This is required for file input and defaults to `i` for the generator.
- `--dq-block-keys NAME,...` — key columns for synthesized aggregation; every key must be in `--dq-block-columns`.
- `--dq-block-aggregations AGG,...` — synthesized aggregations containing `sum:column_name` and/or `count`; sum columns must be selected.
- `--dq-block-ast PATH` — external textual AST defining the four aggregation lambdas and output key width.

The caller must provide exactly one of `--dq-block-file` and `--dq-block-generator`. It must also provide either both `--dq-block-keys` and `--dq-block-aggregations`, or `--dq-block-ast`; the two aggregation forms are mutually exclusive.

The mode accepts the existing `--block-size`, `--run-count`, `--num-attempts`, `--no-verify`, `--llvm`, and `--spilling` controls. It accepts only `--mode=all` and `--mode=graph`; no timed reference-only/generator-only path exists. DQ-block-specific options are rejected for every other `-t` mode.

### Input and graph construction

- Opens the file with vendored Arrow/Parquet (`contrib/libs/apache/arrow`, including `parquet/arrow/reader.h`).
- Alternatively, materializes the range `[0, row-limit)` as `Uint32`, shuffles it in RAM, and packages it into Arrow blocks of at most 30,000 rows under the implicit column name `i`. A zero row count or a count greater than `Uint32`'s maximum is rejected.
- Resolves requested columns against the Arrow schema, rejects missing/duplicate names, and maps supported primitive Arrow types to MKQL data slots.
- Handles integer, floating-point, Boolean, UTF-8/binary string, date32/date64, and timestamp physical types. Arrays are cast to the physical Arrow representation expected by MKQL blocks where necessary (for example Boolean to `uint8`). Unsupported complex types fail with an explicit error.
- Uses the requested block size as the Parquet record-batch size, stops precisely at the row limit, retains all selected arrays in RAM, and reports inferred types.
- Builds a block-wide stream type containing one `TBlockType::Many` per selected column plus the scalar `Uint64` block-length column.
- If the custom AST contains an input transform, compiles it into a separate non-LLVM graph, runs it once over the preloaded input block stream, and retains its complete output stream as Arrow datums before constructing the measured aggregation graph. The datums are rewrapped in the aggregation graph's allocator, so the Arrow data stays zero-copy without sharing allocator-owned MKQL wrappers.
- Either synthesizes key extraction, initialization, update, and finalization lambdas from the CLI aggregation description, or loads them from `--dq-block-ast`.
- Builds `DqHashAggregate` through `TKqpProgramBuilder` in both cases.
- Wraps retained arrays and block lengths into `TUnboxedValue` Arrow blocks before measurements. `--run-count` replays the prebuilt blocks without rereading the file.

### Custom aggregation AST

The AST file has the following shape (the older `AsTuple` root is also accepted):

```lisp
'(
    <block-stream transform lambda or ()>
    <extractKey lambda>
    <init lambda>
    <update lambda>
    <finalize lambda>
    (Uint64 '<output-key-width>))
```

- The input transform is either `()` or a single-argument lambda. Its argument is a wide stream containing the selected input columns as Arrow blocks followed by the scalar `Uint64` block-height column. It must return another wide block stream with at least one data column followed by the same kind of height column.
- A per-element transform can return `WideMap` over that stream. Its mapper receives every stream column, including the height, and should normally pass the height through unchanged. The common MKQL compiler expands chunked block output from `WideMap`, keeping generated Arrow buffers within the MKQL size limit.
- A transform which is more convenient to express with scalar operators can use `WideFromBlocks`, convert its scalar stream to a flow for `WideMap`, then return through `FromFlow` and `WideToBlocks`. `ast_generator_example.txt` uses this form for `% 100`; `WideToBlocks` restores correctly sized Arrow blocks and the height column.
- The next four elements must be wide lambdas in the same argument order used by `DqPhyHashCombine`: transformed input columns for `extractKey`; keys followed by input columns for `init`; keys, input columns, and state for `update`; and keys followed by state for `finalize`.
- The last element declares how many leading columns produced by `finalize` constitute the result key. Custom ASTs taken from production plans may therefore need their finalize lambda reordered to emit keys first.
- The loader deliberately accepts only this list/tuple, not a surrounding `DqPhyHashCombine` callable. Each lambda is wrapped in a temporary `return` statement and passed independently through `CompileExpr`.
- At each program-builder callback, the actual typed MKQL argument nodes are converted back to YQL type annotations with `ConvertMiniKQLType`. `UpdateLambdaAllArgumentsTypes` supplies those contextual types, and a `CreateExtCallableTypeAnnotationTransformer`/`CreateTypeAnnotationTransformer` pair annotates the lambda. `MkqlBuildLambda` lowers the stream transform, while `MkqlBuildWideLambda` lowers the aggregation lambdas, to `TRuntimeNode`s.
- No YQL optimizer pipeline is run. Simple UDF and Arrow resolvers backed by the test function registry are installed for type annotation.
- Lambda arities are checked against the arguments supplied by the aggregation builder.
- Transform output types are derived from the returned stream's `TMultiType` and retain their full MKQL item types, including complex types such as structs. The trailing scalar `Uint64` height block is validated and omitted from the aggregation-lambda arguments. Final aggregation output types are also derived from RuntimeNodes, but every final output must currently be a DataSlot or optional DataSlot.

### Measurement isolation

- The dataset, graph, and prebuilt block stream are prepared in the parent.
- Custom input transforms run during this preparation and are not part of the timed aggregation loop. `--run-count` replays the transformed blocks rather than rerunning the transform.
- Every `--num-attempts` run uses `RunForked`, so each child receives the same pristine graph/input through fork copy-on-write.
- Timing covers graph consumption only. Reference timing remains zero, and the best runtime/max RSS are merged with the existing metrics machinery.

### Correctness checking

- Unless `--no-verify` is specified, a separate untimed fork consumes the block `DqHashAggregate` result.
- The retained Arrow input is scalarized lazily into a separate scalar WideCombiner graph.
- For a custom AST, this reference graph uses the exact same four parsed and type-annotated lambdas as `DqHashAggregate`.
- WideCombiner uses memory limit `0`, which selects full aggregation mode. Any nonzero value selects pre-aggregation and can emit partial duplicate groups.
- Results are compared by encoded composite keys and aggregate values. For custom ASTs, the declared key width divides the leading key columns from the remaining aggregate columns. Integer results are exact; floating-point comparisons use a small relative tolerance. Empty strings and optional/null values are handled.

## Example

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-file hits.parquet \
  --dq-block-row-limit 20000 \
  --dq-block-columns CounterID,RegionID,UserID \
  --dq-block-keys CounterID,RegionID \
  --dq-block-aggregations sum:UserID,count \
  --block-size 128 \
  --run-count 2
```

Custom AST example, using the checked-in `ast_example.txt`:

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-file hits.parquet \
  --dq-block-row-limit 10000 \
  --dq-block-columns CounterID,RegionID,UserID \
  --dq-block-ast ast_example.txt \
  --block-size 128
```

Generator and stream-transform example, using `ast_generator_example.txt`:

```bash
ydb/core/kqp/tools/combiner_perf/bin/combiner_perf \
  -t dq-block \
  --dq-block-generator shuffle:42 \
  --dq-block-row-limit 100000 \
  --dq-block-ast ast_generator_example.txt \
  --block-size 30000
```

The generator also supports the synthesized aggregation path, for example `--dq-block-keys i --dq-block-aggregations count`, without `--dq-block-columns`.

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
- Verified the custom AST over 10,000 rows: 130 groups from both `DqHashAggregate` and the scalar WideCombiner reference.
- Verified the custom AST with LLVM enabled over 1,000 rows: 26 groups from both implementations.
- Confirmed that the original synthesized `sum`/`count` path still passes reference verification after adding custom AST support.
- Verified the empty-transform AST over 10,000 rows after changing the root to a quoted list: 130 groups from both implementations.
- Verified the stream-level `WideMap` struct-packing transform over 10,000 rows with an 8,192-row input block size: the transform precomputed the full output stream before timing and both aggregation implementations produced 3,459 groups.
- Verified the input transform with LLVM enabled and `--run-count 2` over 1,000 rows: 338 groups from both implementations.
- Verified the input transform with spilling enabled over 1,000 rows: 338 groups from both implementations.
- Verified `shuffle:42` produces 1,000 generated rows without an explicit column list, and that the synthesized `count` path produces and verifies 1,000 groups.
- Verified the scalar `WideFromBlocks`/`WideToBlocks` transform over the generated input: `% 100` precomputed 1,000 transformed rows and both aggregation implementations produced 100 groups.
- Confirmed that specifying both input sources is rejected and that a generated row count of 4,294,967,296 is rejected as outside the `Uint32` range.
- Confirmed DQ-block-specific options produce an error with another test mode.
- After the rename, rebuilt `ydb/core/kqp/tools/combiner_perf/bin` and verified the synthesized `sum`/`count` path over 1,000 rows through the new `dq-block` CLI and JSON field names.
- `git diff --check` passed before commit.

## Follow-up scope

- Add a block-input `DqHashCombine`/pre-aggregation target later, likely reusing the Parquet loading, type mapping, stream, and graph-lambda construction in `dq_block.cpp`.
- A future performance reference target can replace or supplement the correctness-only scalar WideCombiner path.

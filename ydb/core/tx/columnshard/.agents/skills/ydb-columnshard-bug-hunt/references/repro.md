# Reproduce a candidate through SQL

## Allowed and not allowed

| Allowed | Not allowed |
|---|---|
| `TKikimrRunner` with real threads, query and table clients, any SQL: DDL, UPSERT, SELECT, `ALTER OBJECT`, PRAGMA | calls into ColumnShard, reader or arrow classes |
| `TKikimrSettings` / `AppConfig` knobs | blocking, dropping or injecting actor events |
| client timeouts and cancellation, concurrent sessions | forcing the `lc-buckets` compaction planner |
| a stress loop with many iterations | the testing CS controller for anything except shortening background periods |

Report every knob and PRAGMA a test needs; the user decides whether the trigger counts as reachable.

## Where tests go

Put the test next to the closest suite in `ydb/core/kqp/ut/olap/`. Add a new file to `SRCS` of that directory's `ya.make`.

| Topic | Directory or file |
|---|---|
| scans, limits, readers | `reading/` |
| distinct and filter pushdown | `pushdown/` |
| encodings, dictionary | `storage/` |
| predicate pushdown | `kqp_olap_ut.cpp` |
| indexes | `indexes/` |
| types, JSON | `types/` |

## Knobs that reach common paths

| Knob | Effect |
|---|---|
| `settings.SetColumnShardReaderClassName("TRIVIAL")` / `"SIMPLE"` | production default is TRIVIAL (`ydb/core/tx/columnshard/engines/reader/transaction/tx_scan.cpp`); `TKikimrSettings` defaults to SIMPLE, so run both |
| `AppConfig.MutableColumnShardConfig()->SetMemoryLimitScanPortion(1)` | portions are read in pages instead of in memory; some paths, such as aggregations, stay in memory (`ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/fetching.cpp`) |
| `AppConfig.MutableColumnShardConfig()->SetScanMemoryLimit(N)` | small per-scan memory stages; allocations wait in the limiter |
| `settings.SetColumnShardAlterObjectEnabled(true)` | `ALTER OBJECT ... (TYPE TABLE)` on a standalone table; a table store does not need it |
| `data Utf8 ENCODING(DICT)` in `CREATE TABLE` | dictionary encoding of a column |
| `ALTER OBJECT ... SET (ACTION=ALTER_COLUMN, NAME=j, \`DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME\`=\`SUB_COLUMNS\`, ...)` | JSON sub-columns |
| `PRAGMA Kikimr.OptForceOlapPushdownDistinct = "<alias>"` with `OptEnableOlapPushdownProjections` | DISTINCT pushed into the shard |
| `TExecuteQuerySettings().ClientTimeout(...)` | cancel a running scan |

Find the exact syntax in existing tests with `grep -rn "<knob>" ydb/core/kqp/ut/olap`.

## Checks before you call a test a repro

1. The path is reached. Assert it: a counter that grows, or the query AST (`StatsMode(EStatsMode::Full)`, `GetStats()->GetAst()`) contains the pushed node such as `KqpOlapApply`. A test that passes is evidence only when the path was reached.
2. The data has the needed shape. Read `.sys/primary_index_stats` (for example `Rows` per `EntityName` and `ChunkIdx`) and assert it, so a change of the write path cannot make the test pass silently.
3. The failure is the predicted one: the same failed assert, error or counter. Copy one line of it into the report.
4. The cause is the suspected commit:
   1. Create a second worktree at the same base ref.
   2. Save the test change of the first worktree as a patch, new files included, and apply it to the second one.
   3. In the second worktree, revert the suspected commit without committing. On a conflict keep the code of the parent of `<sha>` in the conflicting hunks and the current code elsewhere; list the resolved hunks in the report.
   4. Run the same test there. It must pass.
5. The result does not depend on the host. A threshold such as "the allocation fails" must hold on any machine: scale the input past what any host can give, or assert a deterministic observable instead.
6. A test error is not a product bug. When a query fails, read the error first: a wrong SQL statement in the test looks the same as a regression.

## Races and cancellation

A stress run is research; only a test that fails reliably goes into the PR.

1. Write a loop: several reader sessions, a writer, random client timeouts. Count successes and cancellations and print them.
2. Run it for at least 10 minutes: `./ya make --build relwithdebinfo -tA <dir> -F '<Suite>::<Test>' --test-disable-timeout 2>&1 | tail`, then again with `--sanitize=thread` or `--sanitize=address`.
3. No crash does not prove the code safe. Report the duration and the counts, and keep the verdict of the candidate.

## Builds

Follow the root `AGENTS.md` for build and test commands. The sanitizer build of `ydb/core/kqp/ut/olap` takes long: start it early, and edit no source until it finishes.

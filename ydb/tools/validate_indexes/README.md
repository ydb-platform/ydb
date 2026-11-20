# Validate secondary indexes

Check a row table against its ordinary global sync, async and unique indexes, including covering columns. Vector, full-text, JSON and local indexes are skipped.

## Usage

```bash
./ya make --build relwithdebinfo ydb/tools/validate_indexes/bin
ydb/tools/validate_indexes/bin/validate-indexes \
    --endpoint grpc://localhost:2135 \
    --database /Root --table my_table
```

Alternatively, install the Python `ydb` package and run `python3 ydb/tools/validate_indexes/validate_indexes.py` with the same arguments.

| Option | Description |
| --- | --- |
| `-e`, `--endpoint` | Required YDB gRPC endpoint. |
| `-d`, `--database` | Required database path. |
| `-t`, `--table` | Required table path, absolute or relative to the database. |
| `--writes-stopped` | Assert that writes have stopped until exit; enable async convergence checks. |
| `--async-retries` | Retries after the first async attempt; default `20`, minimum `1`. |
| `--async-retry-delay` | Seconds between async attempts; default `30`, finite and positive. |
| `-h`, `--help` | Show usage. |

Authentication uses the Python SDK credential environment, or anonymous credentials if unset. Reading `indexImplTable` requires describe/read permissions and may need admin access and `EnableAccessToIndexImplTables`.

## Validation

> **Warning:** No DDL may run on the table, its ancestors or indexes during validation. Wait for ongoing DDL to finish; use a shared lock honored by all DDL actors or an enforced maintenance window. The tool does not acquire a lock.

- **Sync/unique indexes:** One streaming QueryService SELECT checks the main table and all checked sync/unique indexes in a shared Snapshot Read-Only transaction. Concurrent DML is supported; the verdict describes that snapshot.
- **Data:** Full scans compare primary-key, index-key and covering columns against physical index tables. Detects missing, extra or changed rows; handles either or both sides being empty.
- **Values:** NULLs match NULLs. Float/Double comparison uses exact IEEE-754 bits: `+0.0` and `-0.0` differ; NaN sign and payload must match. JSON/YSON use stored bytes; Decimals use SQL grouping.
- **Schema:** Requires ready indexes. Checks definitions and creation identities before and after reads, including same-name recreation; transient DDL changes can still escape detection.
- **Errors:** Interrupted/truncated streams and detected schema changes invalidate the result. Final summaries require complete scans and successful schema checks; output before an error is partial.

## Async indexes

Without `--writes-stopped`, async indexes are skipped and reported as `inconclusive`.

```bash
ydb/tools/validate_indexes/bin/validate-indexes \
    --endpoint grpc://localhost:2135 --database /Root --table my_table \
    --writes-stopped --async-retries 20 --async-retry-delay 30
```

- Stop all table mutations, including imports and TTL, and wait for commits before starting. Keep writes stopped and administrative index writes excluded until exit. The flag does not block writes; background async replay may continue.
- Each complete attempt compares **all** checked async indexes with the unchanged main table in one Stale Read-Only query, separate from the sync snapshot.
- Confirmation requires **two consecutive complete attempts with zero differences** across the whole async set. A mismatch resets the count. Defaults allow 21 attempts and ten minutes of waits, plus scan time.
- Read replicas on the main or any checked async implementation table prevent confirmation: stale reads could hide changes. Disable them before the DDL exclusion window.
- Success records observed convergence, not a replication barrier; later replay may change an index. Exhausted retries remain `inconclusive`, since reads cannot distinguish lag from corruption.

## Output

- Lists `Checked index set: ...`, explicit `Skipping ...` messages and per-index/table statuses (`consistent`, `inconsistent`, `inconclusive`). Skipped indexes are outside the verdict.
- Streams JSON differences with the index name, main-table PK, physical index key, values and a missing/extra reason. Changed values produce missing/extra pairs. Float diagnostics include exact hex `comparison_bytes`; temporal values use raw numeric units.
- Reports async progress. On retry exhaustion, prints only the last attempt's differences, marked `inconclusive`; successful convergence discards obsolete diagnostics. Async diagnostics use a temporary disk file.

| Exit code | Meaning |
| --- | --- |
| `0` | All sync/unique indexes match and any async indexes are confirmed, or no supported indexes exist. |
| `1` | A sync/unique mismatch was found; takes precedence over async `inconclusive`. |
| `2` | Validation failed: read/schema error, unready index, interrupted/truncated stream, temporary-storage failure or unexpected execution error. Earlier output is partial. |
| `3` | Async consistency was not confirmed: writes were not asserted stopped, read replicas are configured, or retries were exhausted. |

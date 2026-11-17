# Validate secondary indexes

Check a row table against its ordinary global sync, async and unique indexes.
Vector, full-text, JSON and local indexes are skipped.
An external DDL exclusion mechanism must be held for the entire invocation;
`--ddl-excluded` asserts this precondition. Concurrent DML is supported.

```bash
./ya make --build relwithdebinfo ydb/tools/validate_indexes/bin
ydb/tools/validate_indexes/bin/validate-indexes \
    --endpoint grpc://localhost:2135 \
    --database /Root \
    --table my_table --ddl-excluded
```

Alternatively, install the Python `ydb` package and run the script directly:

```bash
python3 ydb/tools/validate_indexes/validate_indexes.py \
    --endpoint grpc://localhost:2135 --database /Root --table my_table --ddl-excluded
```

The table path can be absolute or relative to the database. Authentication uses
the SDK environment variables, such as `YDB_ACCESS_TOKEN_CREDENTIALS` or
`YDB_USER` / `YDB_PASSWORD`. With no credential variables set, authentication
is anonymous. The account must have permission to describe and read the table
and its `indexImplTable` tables; direct index access may require cluster admin
rights and `EnableAccessToIndexImplTables` on the server.

Schema discovery is not transactional with data snapshot acquisition. Before
invocation, wait for any in-progress DDL to finish and acquire an external
exclusive schema-maintenance lock that every DDL actor honors, or establish
an enforced maintenance window that prevents DDL. Hold exclusion until the
process exits. It must cover the table, its ancestors and all its indexes,
including creation, alteration, renaming, deletion and recreation under the
same name. Permission changes that could undermine exclusion are also excluded.

For example, when every DDL job and validator runs on one host, wrap every
such invocation with `flock --exclusive /path/to/ydb-schema.lock ...`. Across
hosts, use a shared distributed lock honored by every DDL actor or an enforced
DDL maintenance window. `--ddl-excluded` does not acquire or enforce a lock;
it is the caller's assertion that this exclusion already exists. The CLI and
Python `validate(..., ddl_excluded=True)` reject a missing assertion.

The script also compares column, primary-key and index definitions and table
creation identities before and after the scan, including each physical
implementation table. Live statistics, automatic partitioning and changefeed
scan progress/state are excluded from these comparisons.
Detected schema changes, same-name recreation, or unavailable creation
identities are errors. These checks are additional protection: public describes
do not expose a monotonic DDL revision and cannot detect every transient
create/drop sequence. They do not replace external DDL exclusion. Ordinary
inserts, updates, deletes, TTL expiration and automatic partitioning may continue.

The output lists the exact discovered supported index set and its types as
`Checked index set: ...`; unsupported indexes are explicitly reported as
`Skipping ...`. Every supported index must be ready. The verdict covers only
this listed set, with skipped indexes excluded. No supported indexes means
there is nothing to validate, not a claim about skipped index contents.

After discovery, one SELECT compares all checked indexes through the streaming
scan query API. All reads of the main table and all checked sync/unique indexes
share one data snapshot. The verdict describes their contents at that snapshot,
under the schema-stability precondition above. It does not describe a later
state or prove schema stability from data reads.

The SELECT combines each main-table projection and its physical `indexImplTable`
with `UNION ALL`, assigning them +1 and -1 respectively. It groups by the index
and every stored column, including primary keys and covering columns, and
returns groups whose sum is nonzero. This detects missing rows, extra rows and
different values, including when either side is empty. Both sides empty means
no differences. NULLs match NULLs. Native Float/Double values are converted to
their lossless little-endian IEEE-754 bytes before grouping: +0.0 and -0.0
differ, and NaNs match only when their sign, payload and all other stored bits
match. There is no NaN normalization. Decimal values retain SQL grouping
semantics, including matching Decimal NaNs. JSON and YSON covering values are
compared as stored bytes; specialized indexes are skipped.
PostgreSQL values are compared and reported in their text representation.

Async indexes are included in the same scan, but normal replication may lag,
and late changes can be applied with an older MVCC version. A shared scan
version does not make those reads repeatable. Their result is always
`inconclusive`, even when observed values match. Async differences are
observations, not confirmed corruption.

Only differences are streamed to the client. Client memory usage is bounded by
a stream batch; the server scans and groups all indexed data. Indexes that are
still building cause an error. Interrupted or truncated streams are errors;
the script consumes the complete stream and never retries part of a scan with
a new snapshot. Detected schema changes also discard the validation result.

Differences are printed as JSON lines with the index name, table primary key,
index key, row values and a missing/extra reason. A changed value produces a
missing expected row and an extra actual row. Each index gets a summary.
For floating-point differences, `row` retains readable values and signed zero;
non-finite values are strings (`nan`, `inf`, `-inf`). `comparison_bytes` records
the exact little-endian bytes as hex, preserving NaN distinctions in diagnostics.
Consistency summaries are emitted only after the complete scan and final
schema checks succeed. Any output preceding an error is partial and cannot
be used as a successful whole-table verdict.

Exit codes:

- `0`: with external DDL exclusion held, all checked sync/unique indexes are consistent and no async indexes
  require confirmation, or there are no supported indexes.
- `1`: at least one confirmed sync/unique mismatch was found. This takes
  precedence over an inconclusive async result.
- `2`: validation failed, for example because DDL exclusion was not asserted,
  a schema/creation identity changed, a table could not be read, or the scan
  was interrupted or truncated. Any earlier output is partial.
- `3`: async consistency could not be confirmed, with no confirmed sync/unique
  mismatches. Observed async differences are included in the output.

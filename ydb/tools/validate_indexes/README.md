# Validate secondary indexes

Check a row table against its ordinary global sync, async and unique indexes.
Vector, full-text, JSON and local indexes are skipped.

```bash
./ya make --build relwithdebinfo ydb/tools/validate_indexes/bin
ydb/tools/validate_indexes/bin/validate-indexes \
    --endpoint grpc://localhost:2135 \
    --database /Root \
    --table my_table
```

Alternatively, install the Python `ydb` package and run the script directly:

```bash
python3 ydb/tools/validate_indexes/validate_indexes.py \
    --endpoint grpc://localhost:2135 --database /Root --table my_table
```

The table path can be absolute or relative to the database. Authentication uses
the SDK environment variables, such as `YDB_ACCESS_TOKEN_CREDENTIALS` or
`YDB_USER` / `YDB_PASSWORD`. With no credential variables set, authentication
is anonymous. The account must have permission to describe and read the table
and its `indexImplTable` tables; direct index access may require cluster admin
rights and `EnableAccessToIndexImplTables` on the server.

After describing the schema, the script executes one SELECT for all supported
indexes through the streaming scan query API. The main table and all sync and
unique indexes share one snapshot and can be checked while writes continue.
The result describes that snapshot. A failed or truncated stream is
an error; the script never restarts part of the check with a new snapshot.

The SELECT combines each main-table projection and its physical `indexImplTable`
with `UNION ALL`, assigning them +1 and -1 respectively. It groups by the index
and every stored column, including primary keys and covering columns, and
returns groups whose sum is nonzero. This detects missing rows, extra rows and
different values, including when either side is empty. Both sides empty means
no differences. NULLs and NaNs are handled by SQL grouping. JSON and YSON
covering values are compared as stored bytes; specialized indexes are skipped.
PostgreSQL values are compared and reported in their text representation.

Async indexes are included in the same scan, but normal replication may lag,
and late changes can be applied with an older MVCC version. A shared scan
version does not make those reads repeatable. Their result is always
`inconclusive`, even when observed values match. Async differences are
observations, not confirmed corruption.

Only differences are streamed to the client. Client memory usage is bounded by
a stream batch; the server scans and groups all indexed data. Indexes that are
still building cause an error.

Differences are printed as JSON lines with the index name, table primary key,
index key, row values and a missing/extra reason. A changed value produces a
missing expected row and an extra actual row. Each index gets a summary.

Exit codes:

- `0`: all checked sync/unique indexes are consistent and no async indexes
  require confirmation, or there are no supported indexes.
- `1`: at least one confirmed sync/unique mismatch was found. This takes
  precedence over an inconclusive async result.
- `2`: validation failed, for example because a table could not be read or the
  snapshot transaction was lost. Any earlier output is partial.
- `3`: async consistency could not be confirmed, with no confirmed sync/unique
  mismatches. Observed async differences are included in the output.

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

Sync and unique indexes can be checked while writes continue. All key pages
and point reads of the main table and these indexes share one `SnapshotReadOnly`
transaction. The result describes that snapshot, including rows deleted or
changed after validation started. If the transaction expires or is lost, the
script returns an error; it never resumes with a new snapshot.

Async indexes have different guarantees: YDB does not allow reading their
`indexImplTable` in a consistent SQL transaction, and normal replication may
lag. The script reads them with `ReadTable` and reports observations, but always
marks their result `inconclusive`, even when observed values match. It never
claims an atomic result or identifies async lag as confirmed corruption.

The script enumerates primary keys from both sides, then reads one row at a time
by full primary key and compares all columns stored in the index, including
covering columns. Sync key enumeration uses bounded pages within the same
transaction. Reading both directions detects missing and extra index rows even
when either table is empty. Memory usage is bounded by a page or stream batch
and the rows being compared; expect several requests per row. A mismatch may
be reported in both directions. Indexes that are still building cause an error.

Mismatches are printed as JSON lines with the index name, table primary key,
index key and missing row or differing values. Each index gets a summary.

Exit codes:

- `0`: all checked sync/unique indexes are consistent and no async indexes
  require confirmation, or there are no supported indexes.
- `1`: at least one confirmed sync/unique mismatch was found. This takes
  precedence over an inconclusive async result.
- `2`: validation failed, for example because a table could not be read or the
  snapshot transaction was lost. Any earlier output is partial.
- `3`: async consistency could not be confirmed, with no confirmed sync/unique
  mismatches. Observed async differences are included in the output.

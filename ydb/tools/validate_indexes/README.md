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

Stop writes and wait for async indexes to catch up before running. Each read
uses its own snapshot; validation does not hold a shared snapshot across tables.
Concurrent writes or async replication lag can appear as inconsistencies.
Indexes that are still building cause an error.

The script streams primary keys from both the table and each index, then reads
one row at a time by full primary key using `ReadTable`. It compares all columns
stored in the index, including covering columns. Reading both directions detects
missing and extra index rows even when either table is empty. Memory usage is
bounded by a stream batch and the rows being compared; expect several requests
per row. A mismatch may be reported in both directions.

Mismatches are printed as JSON lines with the index name, table primary key,
index key and missing row or differing values. Each index gets a summary.

Exit codes:

- `0`: all checked indexes are consistent, or there are no supported indexes.
- `1`: at least one mismatch was found.
- `2`: validation failed, for example because a table could not be read.

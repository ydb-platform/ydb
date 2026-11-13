# IMDb Join Order Benchmark

This workload uses the same benchmark and file-import framework as ClickBench.
It contains all 113 numbered queries and all 21 tables from
[gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark).
The benchmark comes from *How Good Are Query Optimizers, Really?* by Viktor Leis,
Andrey Gubichev, Atans Mirchev, Peter Boncz, Alfons Kemper, and Thomas Neumann
(PVLDB 9(3), 2015).

## Schema

`schema.sql` is the standalone YQL translation of upstream `schema.sql`.
`imdb_schema.yaml` describes the same columns for the shared DDL generator:
PostgreSQL `integer` becomes `Int32`, and `text` and `varchar(n)` become `Utf8`
(or `String` with `init --string`). Length limits are not enforced.
The YAML records the original primary keys and nullability. As with ClickBench,
the shared DDL generator emits `NOT NULL` for column storage and omits it for
row storage. The standalone SQL preserves every original `NOT NULL` constraint.

Tables default to column storage and live under the `imdb` path. Use `--path`
to select a different directory and `init --store row` for row storage.

```sh
ydb -e grpc://localhost:2135 -d /Root workload imdb init
ydb -e grpc://localhost:2135 -d /Root workload imdb import files --input /data/imdb
ydb -e grpc://localhost:2135 -d /Root workload imdb run
ydb -e grpc://localhost:2135 -d /Root workload imdb run --include 1a,17f,33c
ydb -e grpc://localhost:2135 -d /Root workload imdb clean
```

## Queries

`queries/` contains the upstream numbered SQL files without modifications.
`queries.list` fixes their order by query number and variant, from `1a` to `33c`.
The runner sets `TablePathPrefix` to the selected database and workload path,
and enables `AnsiImplicitCrossJoin` to support the upstream comma-separated
tables with join predicates in `WHERE`. The standard benchmark options provide
query selection, iterations, and reports. Canonical result checking is not
available; `--check-canonical` reports an error.

## Data import

After a successful database import, the runner executes `ANALYZE` on all 21
tables to collect statistics for every column. This applies to both row and
column tables and uses the configured `--path`. Import waits for each table's
statistics collection to finish and reports an error if it fails. File-only
exports (`--file-output-path`) skip this step.

To collect statistics for an already completed import, rerun the import command
with the same `--state` file: completed data portions are skipped, and `ANALYZE`
runs again. Statistics are collected after loading, rather than on empty tables
during `init`.

Supply all 21 tables as headerless `<table>.csv`, `<table>.csv.gz`, `<table>.tsv`,
or `<table>.tsv.gz` files, or as `<table>.parquet` files (Linux). Alternatively,
each table can have a subdirectory containing parts in these formats. Text
columns must appear in the order in `schema.sql`; the importer supplies headers.
All matching files are imported, so keep only one copy of each table's data.
Missing tables and unsupported files inside table directories are errors.

CSV import supports the original JOB dataset's backslash-escaped quotes and
backslashes, quoted multiline fields, and empty unquoted fields as NULL. It
normalizes complete records to YDB's CSV format before sending them, and supports
resuming with `--state`. TSV import uses the shared ClickBench importer and
requires one record per physical line. The upstream README links to the May 2013 dataset
used in the original benchmark; newer IMDb exports may produce different results.

## Tests

```sh
./ya make --build relwithdebinfo -tA ydb/library/workload/imdb/ut 2>&1 | tail
```

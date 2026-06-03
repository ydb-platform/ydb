# Importing and exporting data with federated queries

{% note info %}

When importing or exporting data to or from S3 in Parquet format, take into account the [YQL and Apache Arrow type mapping](s3/arrow_types_mapping.md).

{% endnote %}

## Importing data {#import}

Federated queries let you import data from connected external sources into {{ ydb-full-name }} tables. To import data, use a read query from an [external data source](../../datamodel/external_data_source.md) or [external table](../../datamodel/external_table.md) and write into a {{ ydb-short-name }} table.

For column-oriented tables, massively parallel import from an external source is supported when using `UPSERT` and `INSERT`: several worker threads read data from the external source in parallel and write into the table. For row-oriented tables, this functionality is under development.

|Operation|Writes to [row-oriented tables](../../datamodel/table.md#row-oriented-tables)|Writes to [column-oriented tables](../../datamodel/table.md#column-oriented-tables)|
|--------|-----------------|------------------|
|[UPSERT](../../../yql/reference/syntax/upsert_into.md)|single-threaded|parallel|
|[REPLACE](../../../yql/reference/syntax/replace_into.md)|single-threaded|parallel|
|[INSERT](../../../yql/reference/syntax/insert_into.md)|single-threaded|parallel|

{% note tip %}

The recommended import options using federated queries are `UPSERT` and `REPLACE` — the import path is heavily optimized for them.

{% endnote %}

Example: import data from a PostgreSQL table into a {{ ydb-short-name }} table:

```yql
UPSERT INTO target_table
SELECT * FROM postgresql_datasource.source_table
```

For more on creating external data sources and external tables, and on read queries, see:

- [ClickHouse](clickhouse.md#query)
- [Greenplum](greenplum.md#query)
- [Microsoft SQL Server](ms_sql_server.md#query)
- [MySQL](mysql.md#query)
- [PostgreSQL](postgresql.md#query)
- [S3](s3/external_table.md)
- [{{ ydb-short-name }}](ydb.md#query)

## Exporting data {#export}

Currently, exporting data with federated queries is supported only for S3-compatible storage; see [{#T}](s3/write_data.md#export-to-s3).

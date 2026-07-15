# Import and export data using federated queries

{% note info %}

When importing/exporting data from/to S3 in Parquet format, you must consider the [mapping of YQL and Apache Arrow types](s3/arrow_types_mapping.md).

{% endnote %}

## Data import {#import}

Federated queries allow you to import data from connected external sources into {{ ydb-full-name }} database tables. To import data, you need to use a read query from an [external data source](../../datamodel/external_data_source.md) or [external table](../../datamodel/external_table.md) and write to a {{ ydb-short-name }} table.

For columnar tables, massively parallel import from an external source is supported when using UPSERT and INSERT statements — multiple worker threads read data from the external source in parallel and write to the table. For row-based tables, this functionality is under development.

| Operation | Write to [row-based tables](../../datamodel/table.md#row-oriented-tables) | Write to [columnar tables](../../datamodel/table.md#column-oriented-tables) |
| --- | --- | --- |
| [UPSERT](../../../yql/reference/syntax/upsert_into.md) | single-threaded | parallel |
| [REPLACE](../../../yql/reference/syntax/replace_into.md) | single-threaded | parallel |
| [INSERT](../../../yql/reference/syntax/insert_into.md) | single-threaded | parallel |

{% note tip %}

The recommended import options using federated queries are the `UPSERT` and `REPLACE` operations — the data import scenario is significantly optimized for them.

{% endnote %}

Example of importing data from a PostgreSQL table into a {{ ydb-short-name }} table:


```yql
UPSERT INTO target_table
SELECT * FROM postgresql_datasource.source_table
```


For more details on creating external data sources and external tables, as well as reading data from them, see the articles:

{% include [!](_includes/supported_eds.md) %}

{% cut "Experimental data sources" %}

{% include [!](_includes/experimental_eds.md) %}

{% endcut %}

## Data export {#export}

Currently, data export using federated queries is supported only for S3-compatible storage; for more details, see the article [{#T}](s3/write_data.md#export-to-s3).

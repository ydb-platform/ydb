# WITH

It's set after the data source in `FROM` and is used for additional hints for tables. You can't use hints for subqueries and [named expressions](../expressions.md#named-nodes).

The following values are supported:

* `INFER_SCHEMA`: Sets the flag for output of the table schema. The behavior is similar to the [yt.inferSchema pragma](../pragma.md#inferschema), but for a specific data source. You can specify the number of rows to output (from 1 to 1000).
* `FORCE_INFER_SCHEMA`: Sets the flag for table schema output. The behavior is similar to the [yt.ForceInferSchema pragma](../pragma.md#inferschema), but for a specific data source. You can specify the number of rows to output (from 1 to 1000).
* `DIRECT_READ`: Suppresses certain optimizers and enforces accessing table contents as is. The behavior is similar to the debug [pragma DirectRead](../pragma.md#debug), but for a specific data source.
* `INLINE`: Hints that the table contents is small and you need to use its in-memory view to process the query. The actual size of the table is not controlled in this case, and if it's large, the query might fail with an out-of-memory error.
* `UNORDERED`: Suppresses original table sorting.
* `XLOCK`: Hints that you need to lock the table exclusively. It's useful when you read a table at the stage of processing the [query metaprogram](../action.md), and then update its contents in the main query. Avoids data loss if an external process managed to change the table between executing a metaprogram phase and the main part of the query.
* `SCHEMA` type: Hints that the specified table schema must be used entirely, ignoring the schema in the metadata.
* `COLUMNS` type: Hints that the specified types should be used for columns whose names match the table's column names in the metadata, as well as which columns are additionally present in the table.
* `IGNORETYPEV3`, `IGNORE_TYPE_V3`: Sets the flag to ignore type_v3 types in the table. The behavior is similar to the [yt.IgnoreTypeV3 pragma](../pragma.md#ignoretypev3), but for a specific data source.

{% if feature_federated_queries %}

When working with [external file data sources](../../../../concepts/datamodel/external_data_source.md), you can specify additional parameters:

* `FORMAT` — stored data format in file storage for [federated queries](../../../../concepts/query_execution/federated_query/s3/formats.md). Allowed values: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` — file compression in file storage for [federated queries](../../../../concepts/query_execution/federated_query/s3/formats.md#compression). Allowed values: [gzip](https://en.wikipedia.org/wiki/Gzip), [zstd](https://en.wikipedia.org/wiki/Zstd), [lz4](https://en.wikipedia.org/wiki/LZ4), [brotli](https://en.wikipedia.org/wiki/Brotli), [bzip2](https://en.wikipedia.org/wiki/Bzip2), [xz](https://en.wikipedia.org/wiki/XZ_(compression_algorithm)).
* `PARTITIONED_BY` — list of [partition columns](../../../../concepts/query_execution/federated_query/s3/partitioning.md) for data in file storage in federated queries. Lists columns in the order they appear in the file layout.
* `projection.enabled` — flag to enable [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Allowed values: `true`, `false`.
* `projection.<field_name>.type` — field type for [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Allowed values: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` — extended properties of a field for [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md).

{% endif %}

When setting the `SCHEMA` and `COLUMNS` hints, the type must be a [structure](../../types/containers.md).

{% if feature_bulk_tables %}

If you use the `SCHEMA` hint, then with the table functions [EACH](concat.md#each), [RANGE](concat.md#range), [LIKE](concat.md#like), [REGEXP](concat.md#regexp), [FILTER](concat.md#filter) you can use an empty list of tables that is treated as an empty table with columns defined in the `SCHEMA`.

{% endif %}

## Examples

```yql
SELECT key FROM my_table WITH INFER_SCHEMA;
SELECT key FROM my_table WITH FORCE_INFER_SCHEMA="42";
```

```yql
$s = (SELECT COUNT(*) FROM my_table WITH XLOCK);

INSERT INTO my_table WITH TRUNCATE
SELECT EvaluateExpr($s) AS a;
```

```yql
SELECT key, value FROM my_table WITH SCHEMA Struct<key:String, value:Int32>;
```

```yql
SELECT key, value FROM my_table WITH COLUMNS Struct<value:Int32?>;
```

```yql
SELECT key, value FROM EACH($my_tables) WITH SCHEMA Struct<key:String, value:List<Int32>>;
```

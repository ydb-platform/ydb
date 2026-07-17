# WITH

Specified after the data source in `FROM` and used to provide additional table usage hints. Hints cannot be specified for subqueries and [named expressions](../expressions.md#named-nodes).

The following values are supported:

* `INFER_SCHEMA` — sets the flag for inferring the table schema. The behavior is similar to setting the [yt.InferSchema pragma](../pragma.md#inferschema), but only for a specific data source. You can specify the number of rows to infer (a number from 1 to 1000).
* `FORCE_INFER_SCHEMA` — sets the flag for forcing the table schema inference. The behavior is similar to setting the [yt.ForceInferSchema pragma](../pragma.md#inferschema), but only for a specific data source. You can specify the number of rows to infer (a number from 1 to 1000).
* `DIRECT_READ` — suppresses some optimizers and forces using the table contents as is. The behavior is similar to setting the [DirectRead debug pragma](../pragma.md#debug), but only for a specific data source.
* `INLINE` — indicates that the table contents are small and should be represented in memory for query processing. The actual table size is not controlled, and if it is large, the query may fail due to memory exhaustion.
* `UNORDERED` — suppresses the use of the original table sorting.
* `XLOCK` — indicates that an exclusive lock should be taken on the table. Useful when the table is read during the [query metaprogram](../action.md) processing stage and then its contents are updated in the main query. It helps avoid data loss if an external process modifies the table between the execution of the metaprogram phase and the main part of the query.
* `SCHEMA` type — indicates that the specified table schema should be used entirely, ignoring the schema in the metadata.
* `COLUMNS` type — indicates that the specified types should be used for columns whose names match the column names in the table metadata, as well as which additional columns are present in the table.
* `IGNORETYPEV3`, `IGNORE_TYPE_V3` — sets the flag for ignoring type_v3 types in the table. The behavior is similar to setting the [yt.IgnoreTypeV3 pragma](../pragma.md#ignoretypev3), but only for a specific data source.

{% if feature_federated_queries %}

When working with [external file data sources](../../../../concepts/datamodel/external_data_source.md), you can additionally specify a number of parameters:

* `FORMAT` - format of stored data in file storages in [federated queries](../../../../concepts/query_execution/federated_query/s3/formats.md). Valid values: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` — file compression format in file storage for [federated queries](../../../../concepts/query_execution/federated_query/s3/partition_projection). Valid values: [gzip](https://ru.wikipedia.org/wiki/Gzip), [zstd](https://ru.wikipedia.org/wiki/Zstandard), [lz4](https://ru.wikipedia.org/wiki/LZ4), [brotli](https://ru.wikipedia.org/wiki/Brotli), [bzip2](https://ru.wikipedia.org/wiki/Bzip2), [xz](https://ru.wikipedia.org/wiki/XZ).
* `PARTITIONED_BY` - list of [partitioning columns](../../../../concepts/query_execution/federated_query/s3/partitioning.md) of data in file storages in federated queries. Contains a list of columns in the order they are placed in the file storage.
* `projection.enabled` - flag for enabling [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Valid values: `true`, `false`.
* `projection.<field_name>.type` - field type for [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Valid values: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - extended properties of the field for [extended data partitioning](../../../../concepts/query_execution/federated_query/s3/partition_projection.md).

{% endif %}

When reading from a [topic](../../../../concepts/datamodel/topic.md) in [streaming queries](../../../../dev/streaming-query/index.md), you can specify watermark parameters:

{% include notitle [x](../../../../_includes/watermark_parameters.md) %}

When specifying the `SCHEMA` and `COLUMNS` hints, the value of the type parameter must be a [structure](../../types/containers.md) type.

{% if feature_bulk_tables %}

If the `SCHEMA` hint is specified, when using the table functions [EACH](concat.md), [RANGE](concat.md), [LIKE](concat.md), [REGEXP](concat.md), [FILTER](concat.md), an empty table list is allowed, which is treated as an empty table with columns described in `SCHEMA`.

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


```yql
SELECT
    *
FROM
    my_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        ts String
    ),
    WATERMARK = __ydb_write_time - Interval("PT5S"),
    WATERMARK_GRANULARITY = "PT1S",
    WATERMARK_IDLE_TIMEOUT = "PT5S"
);
```

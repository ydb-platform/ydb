## YDB data source parameters

{% if feature_federated_queries %}

When working with [external file data sources](../../../../../concepts/datamodel/external_data_source.md), you can additionally specify a number of parameters:

* `FORMAT`: data storage format in file storages in [federated queries](../../../../../concepts/query_execution/federated_query/s3/formats.md). Allowed values: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION`: file compression format in file storages in [federated queries](../../../../../concepts/query_execution/federated_query/s3/partition_projection). Valid values: [gzip](https://en.wikipedia.org/wiki/Gzip), [zstd](https://en.wikipedia.org/wiki/Zstd), [lz4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)), [brotli](https://en.wikipedia.org/wiki/Brotli), [bzip2](https://en.wikipedia.org/wiki/Bzip2), [xz](https://en.wikipedia.org/wiki/XZ_Utils).
* `PARTITIONED_BY` - a list of [partitioning columns](../../../../../concepts/query_execution/federated_query/s3/partitioning.md) of data in file storages in federated queries. Contains a list of columns in the order they are placed in the file storage.
* `projection.enabled` - a flag for enabling [extended data partitioning](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Valid values: `true`, `false`.
* `projection.<field_name>.type` - field type of [extended data partitioning](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Valid values: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - extended properties of the field of [extended data partitioning](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md).

{% endif %}

When reading from a [topic](../../../../../concepts/datamodel/topic.md) in [streaming queries](../../../../../dev/streaming-query/index.md), you can specify watermarks parameters:

{% include notitle [x](../../../../../_includes/watermark_parameters.md) %}

### Reading a topic

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

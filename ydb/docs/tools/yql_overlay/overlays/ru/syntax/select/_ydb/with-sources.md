## Параметры источников данных YDB

{% if feature_federated_queries %}

При работе с [внешними файловыми источниками данных](../../../../../concepts/datamodel/external_data_source.md) можно дополнительно указывать ряд параметров:

* `FORMAT` - формат хранимых данных в файловых хранилищах в [федеративных запросах](../../../../../concepts/query_execution/federated_query/s3/formats.md). Допустимые значения: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` - формат сжатия файлов в файловых хранилищах в [федеративных запросах](../../../../../concepts/query_execution/federated_query/s3/partition_projection). Допустимые значения: [gzip](https://ru.wikipedia.org/wiki/Gzip), [zstd](https://ru.wikipedia.org/wiki/Zstandard), [lz4](https://ru.wikipedia.org/wiki/LZ4), [brotli](https://ru.wikipedia.org/wiki/Brotli), [bzip2](https://ru.wikipedia.org/wiki/Bzip2), [xz](https://ru.wikipedia.org/wiki/XZ).
* `PARTITIONED_BY` - список [колонок партиционирования](../../../../../concepts/query_execution/federated_query/s3/partitioning.md) данных в файловых хранилищах в федеративных запросах. Содержит список колонок в порядке их размещения в файловом хранилище.
* `projection.enabled` - флаг включения [расширенного партиционирования данных](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `true`, `false`.
* `projection.<field_name>.type` - тип поля [расширенного партиционирования данных](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - расширенные свойства поля [расширенного партиционирования данных](../../../../../concepts/query_execution/federated_query/s3/partition_projection.md).
{% endif %}

При чтении из [топика](../../../../../concepts/datamodel/topic.md) в [потоковых запросах](../../../../../dev/streaming-query/index.md) можно указывать параметры watermarks:

{% include notitle [x](../../../../../_includes/watermark_parameters.md) %}

### Пример чтения топика

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

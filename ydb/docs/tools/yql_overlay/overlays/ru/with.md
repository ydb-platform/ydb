{% include [common](_yql_with.md) %}

## Дополнительные подсказки

* `INFER_SCHEMA` — задает флаг вывода схемы таблицы. Поведение аналогично заданию прагмы yt.InferSchema, только для конкретного источника данных. Можно задать число строк для выведения (число от 1 до 1000).
* `FORCE_INFER_SCHEMA` — задает флаг вывода схемы таблицы. Поведение аналогично заданию прагмы yt.ForceInferSchema, только для конкретного источника данных. Можно задать число строк для выведения (число от 1 до 1000).
* `DIRECT_READ` — подавляет работу некоторых оптимизаторов и заставляет использовать содержимое таблицы как есть. Поведение аналогично заданию отладочной {% if tech and feature_webui %}[прагмы DirectRead](../pragma.md#debug){% else %}прагмы DirectRead{% endif %}, только для конкретного источника данных.
* `XLOCK` — указание на то, что нужно брать эксклюзивный лок на таблицу. Полезен, когда чтение таблицы происходит на стадии обработки [метапрограммы запроса](../action.md), а затем ее содержимое обновляется в основном запросе. Позволяет избежать потери данных, если между исполнением фазы метапрограммы и основной частью запроса внешний процесс успел изменить таблицу.
* `IGNORETYPEV3`, `IGNORE_TYPE_V3` — задает флаг игнорирования type_v3 типов в таблице. Поведение аналогично заданию прагмы yt.IgnoreTypeV3, только для конкретного источника данных.

## Дополнительные параметры источников

{% if feature_federated_queries %}

При работе с [внешними файловыми источниками данных](../../../../concepts/datamodel/external_data_source.md) можно дополнительно указывать ряд параметров:

* `FORMAT` - формат хранимых данных в файловых хранилищах в [федеративных запросах](../../../../concepts/query_execution/federated_query/s3/formats.md). Допустимые значения: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` - формат сжатия файлов в файловых хранилищах в [федеративных запросах](../../../../concepts/query_execution/federated_query/s3/partition_projection). Допустимые значения: [gzip](https://ru.wikipedia.org/wiki/Gzip), [zstd](https://ru.wikipedia.org/wiki/Zstandard), [lz4](https://ru.wikipedia.org/wiki/LZ4), [brotli](https://ru.wikipedia.org/wiki/Brotli), [bzip2](https://ru.wikipedia.org/wiki/Bzip2), [xz](https://ru.wikipedia.org/wiki/XZ).
* `PARTITIONED_BY` - список [колонок партиционирования](../../../../concepts/query_execution/federated_query/s3/partitioning.md) данных в файловых хранилищах в федеративных запросах. Содержит список колонок в порядке их размещения в файловом хранилище.
* `projection.enabled` - флаг включения [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `true`, `false`.
* `projection.<field_name>.type` - тип поля [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - расширенные свойства поля [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md).
{% endif %}

При чтении из [топика](../../../../concepts/datamodel/topic.md) в [потоковых запросах](../../../../dev/streaming-query/index.md) можно указывать параметры watermarks:

{% include notitle [x](../../../../_includes/watermark_parameters.md) %}

## Дополнительные примеры

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

# WITH

Задается после источника данных во `FROM` и используется для указания дополнительных подсказок использования таблиц. Подсказки нельзя задать для подзапросов и [именованных выражений](../expressions.md#named-nodes).

Поддерживаются следующие значения:

* `INFER_SCHEMA` — задает флаг вывода схемы таблицы. Поведение аналогично заданию [прагмы yt.InferSchema](../pragma.md#inferschema), только для конкретного источника данных. Можно задать число строк для выведения (число от 1 до 1000).
* `FORCE_INFER_SCHEMA` — задает флаг вывода схемы таблицы. Поведение аналогично заданию [прагмы yt.ForceInferSchema](../pragma.md#inferschema), только для конкретного источника данных. Можно задать число строк для выведения (число от 1 до 1000).
* `DIRECT_READ` — подавляет работу некоторых оптимизаторов и заставляет использовать содержимое таблицы как есть. Поведение аналогично заданию отладочной [прагмы DirectRead](../pragma.md#debug), только для конкретного источника данных.
* `INLINE` — указание на то, что содержимое таблицы небольшое и нужно использовать его представление в памяти для обработки запроса. Реальный объем таблицы при этом не контролируется, и если он большой, то запрос может упасть по превышению памяти.
* `UNORDERED` — подавляет использование исходной сортировки таблицы.
* `XLOCK` — указание на то, что нужно брать эксклюзивный лок на таблицу. Полезен, когда чтение таблицы происходит на стадии обработки [метапрограммы запроса](../action.md), а затем ее содержимое обновляется в основном запросе. Позволяет избежать потери данных, если между исполнением фазы метапрограммы и основной частью запроса внешний процесс успел изменить таблицу.
* `SCHEMA` type — указание на то, что следует использовать указанную схему таблицы целиком, игнорируя схему в метаданных.
* `COLUMNS` type — указание на то, что следует использовать указанные типы для колонок, чьи имена совпадают с именами колонок таблицы в метаданных, а также какие колонки дополнительно присутствуют в таблице.
* `IGNORETYPEV3`, `IGNORE_TYPE_V3` — задает флаг игнорирования type_v3 типов в таблице. Поведение аналогично заданию [прагмы yt.IgnoreTypeV3](../pragma.md#ignoretypev3), только для конкретного источника данных.

{% if feature_federated_queries %}

При работе с [внешними файловыми источниками данных](../../../../concepts/datamodel/external_data_source.md) можно дополнительно указывать ряд параметров:

* `FORMAT` - формат хранимых данных в файловых хранилищах в [федеративных запросах](../../../../concepts/query_execution/federated_query/s3/formats.md). Допустимые значения: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` - формат сжатия файлов в файловых хранилищах в [федеративных запросах](../../../../concepts/query_execution/federated_query/s3/partition_projection). Допустимые значения: [gzip](https://ru.wikipedia.org/wiki/Gzip), [zstd](https://ru.wikipedia.org/wiki/Zstandard), [lz4](https://ru.wikipedia.org/wiki/LZ4), [brotli](https://ru.wikipedia.org/wiki/Brotli), [bzip2](https://ru.wikipedia.org/wiki/Bzip2), [xz](https://ru.wikipedia.org/wiki/XZ).
* `PARTITIONED_BY` - список [колонок партиционирования](../../../../concepts/query_execution/federated_query/s3/partitioning.md) данных в файловых хранилищах в федеративных запросах. Содержит список колонок в порядке их размещения в файловом хранилище.
* `projection.enabled` - флаг включения [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `true`, `false`.
* `projection.<field_name>.type` - тип поля [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md). Допустимые значения: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - расширенные свойства поля [расширенного партиционирования данных](../../../../concepts/query_execution/federated_query/s3/partition_projection.md).

{% endif %}

При задании подсказок `SCHEMA` и `COLUMNS` в качестве значения типа type должен быть задан тип [структуры](../../types/containers.md).

{% if feature_bulk_tables %}

Если задана подсказка `SCHEMA`, то при использовании табличных функций [EACH](concat.md), [RANGE](concat.md), [LIKE](concat.md), [REGEXP](concat.md), [FILTER](concat.md) допускается пустой список таблиц, который обрабатывается как пустая таблица с колонками, описанными в `SCHEMA`.

{% endif %}

## Примеры

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

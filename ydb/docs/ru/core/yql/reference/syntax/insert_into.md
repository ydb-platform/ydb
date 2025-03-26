
# INSERT INTO

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../_includes/not_allow_for_olap_text.md) %}

{% include [ways_add_data_to_olap](../../../_includes/ways_add_data_to_olap.md) %}

{% endnote %}

{% endif %}

{% if select_command != "SELECT STREAM" %}
Добавляет строки в {% if oss == true and backend_name == "YDB" %}строковую{% endif %} таблицу.{% if feature_bulk_tables %} Если целевая таблица уже существует и не является сортированной, операция `INSERT INTO` дописывает строки в конец таблицы. В случае сортированной таблицы, YQL пытается сохранить сортированность путем запуска сортированного слияния. {% endif %}{% if feature_map_tables %} При попытке вставить в таблицу строку с уже существующим значением первичного ключа операция завершится ошибкой с кодом `PRECONDITION_FAILED` и текстом `Operation aborted due to constraint violation: insert_pk`.{% endif %}

{% if feature_mapreduce %}Таблица по имени ищется в базе данных, заданной оператором [USE](use.md).{% endif %}

`INSERT INTO` позволяет выполнять следующие операции:

* Добавление константных значений с помощью [`VALUES`](values.md).

  ```yql
  INSERT INTO my_table (Key1, Key2, Value1, Value2)
  VALUES (345987,'ydb', 'Яблочный край', 1414);
  COMMIT;
  ```

  ```yql
  INSERT INTO my_table (key, value)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Сохранение результата выборки `SELECT`.

  ```yql
  INSERT INTO my_table
  SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
  FROM my_table1;
  ```

{% else %}

Направить результат вычисления [SELECT STREAM](select_stream.md) в указанный стрим на кластере, заданном оператором [USE](use.md). Стрим должен существовать и иметь схему, подходящую результату запроса.

## Примеры

```yql
INSERT INTO my_stream_dst
SELECT STREAM key FROM my_stream_src;
```

Существует возможность указать в качестве цели таблицу на кластере ydb. Таблица должна существовать на момент создания операции. Схема таблицы должна быть совместима с типом результата запроса.

## Примеры

```yql
INSERT INTO ydb_cluster.`my_table_dst`
SELECT STREAM * FROM rtmr_cluster.`my_stream_source`;
```

{% endif %}


{% if feature_insert_with_truncate %}

Запись может выполняться с одним или несколькими модификаторами. Модификатор указывается после ключевого слова `WITH` после имени таблицы: `INSERT INTO ... WITH SOME_HINT`.
Если у модификатора есть значение, то оно указывается после знака `=`: `INSERT INTO ... WITH SOME_HINT=value`.
Если необходимо указать несколько модификаторов, то они заключаются в круглые скобки: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

Чтобы перед записью очистить таблицу от имевшихся данных, достаточно добавить модификатор: `INSERT INTO ... WITH TRUNCATE`.

## Примеры

```yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

{% endif %}


{% if feature_federated_queries %}

При работе с [внешними файловыми источниками данных](../../../concepts/datamodel/external_data_source.md) можно дополнительно указывать ряд параметров:

* `FORMAT` - формат хранимых данных в файловых хранилищах в [федеративных запросах](../../../concepts/federated_query/s3/formats.md). Допустимые значения: `csv_with_names`, `tsv_with_names`, `json_list`, `json_each_row`, `json_as_string`, `parquet`, `raw`.
* `COMPRESSION` - формат сжатия файлов в файловых хранилищах в [федеративных запросах](../../../concepts/federated_query/s3/partition_projection). Допустимые значения: [gzip](https://ru.wikipedia.org/wiki/Gzip), [zstd](https://ru.wikipedia.org/wiki/Zstandard), [lz4](https://ru.wikipedia.org/wiki/LZ4), [brotli](https://ru.wikipedia.org/wiki/Brotli), [bzip2](https://ru.wikipedia.org/wiki/Bzip2), [xz](https://ru.wikipedia.org/wiki/XZ).
* `PARTITIONED_BY` - список [колонок партиционирования](../../../concepts/federated_query/s3/partitioning.md) данных в файловых хранилищах в федеративных запросах. Содержит список колонок в порядке их размещения в файловом хранилище.
* `projection.enabled` - флаг включения [расширенного партиционирования данных](../../../concepts/federated_query/s3/partition_projection.md). Допустимые значения: `true`, `false`.
* `projection.<field_name>.type` - тип поля [расширенного партиционирования данных](../../../concepts/federated_query/s3/partition_projection.md). Допустимые значения: `integer`, `enum`, `date`.
* `projection.<field_name>.<options>` - расширенные свойства поля [расширенного партиционирования данных](../../../concepts/federated_query/s3/partition_projection.md).


## Пример

```yql
INSERT INTO `connection`.`test/`
WITH
(
  FORMAT = "csv_with_names"
)
SELECT
    "value" AS value, "name" AS name
```

Где:

* `connection` — название соединения с S3 ({{ objstorage-full-name }}).
* `test/`— путь внутри бакета, куда будут записаны данные. При записи создаются файлы со случайными именами.

{% endif %}


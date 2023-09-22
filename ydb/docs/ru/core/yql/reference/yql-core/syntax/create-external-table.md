# CREATE EXTERNAL TABLE

Вызов `CREATE EXTERNAL TABLE` создает [внешнюю таблицу](../../../concepts/datamodel/external_table.md) с указанной схемой данных.

```sql
CREATE EXTERNAL TABLE table_name (
  column1 type1,
  column2 type2 NOT NULL,
  ...
  columnN typeN NULL
) WITH (
  DATA_SOURCE="data_source_name",
  LOCATION="path",
  FORMAT="format_name",
  COMPRESSION="compression_name"
);
```

Где:
* `column1 type1`, `columnN typeN NULL` - колонка данных и ее тип;
* `data_source_name` - имя [подключения](../../../concepts/datamodel/external_data_source.md) к S3 ({{ objstorage-name }}).
* `path` - путь внутри бакета с данными. Путь должен вести на существующий каталог внутри бакета.
* `format_name` - один из [допустимых типов хранения данных](../../../concepts/federated_query/s3/formats.md).
* `compression_name` - один из [допустимых алгоритмов сжатия](../../../concepts/federated_query/s3/formats.md#compression).


Допускается использование только ограниченного подмножества типов данных:
- `Bool`.
- `Int8`, `Uint8`, `Int16`, `Uint16`, `Int32`, `Uint32`, `Int64`, `Uint64`.
- `Float`, `Double`.
- `Date`, `DateTime`.
- `String`, `Utf8`.

Без дополнительных модификаторов колонка приобретает [опциональный тип](../types/optional.md) тип, и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.

**Пример**

Cледующий SQL-запрос создает внешнюю таблицу с именем `s3_test_data`, в котором расположены файлы в формате `CSV` со строковыми полями `key` и `value`, находящиеся внутри бакета по пути `test_folder`, при этом для указания реквизитов подключения используется объект [подключение](../../../concepts/datamodel/external_data_source.md) `bucket`:

```sql
CREATE EXTERNAL TABLE s3_test_data (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```




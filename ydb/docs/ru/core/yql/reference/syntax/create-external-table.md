# CREATE EXTERNAL TABLE

Вызов `CREATE EXTERNAL TABLE` создает [внешнюю таблицу](../../../concepts/datamodel/external_table.md) с указанной схемой данных.

```yql
CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] table_name (
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

* `OR REPLACE` - если внешняя таблица с таким именем уже существует, она будет заменена новым определением; версия объекта при этом увеличивается.
* `IF NOT EXISTS` - не выводить ошибку, если внешняя таблица с таким именем уже существует; существующая таблица останется без изменений.
* `column1 type1`, `columnN typeN NULL` - колонка данных и ее тип;
* `data_source_name` - имя [подключения](../../../concepts/datamodel/external_data_source.md) к S3 ({{ objstorage-name }}).
* `path` - путь к файлу, префикс каталога с завершающим `/` или шаблон пути внутри бакета с данными.
* `format_name` - один из [допустимых типов хранения данных](../../../concepts/query_execution/federated_query/s3/formats.md).
* `compression_name` - один из [допустимых алгоритмов сжатия](../../../concepts/query_execution/federated_query/s3/formats.md#compression).


Параметр `VALIDATE_LOCATION="true"` в `WITH` включает проверку существования пути в S3 перед созданием таблицы. Проверка использует учетные данные внешнего источника и требует права на получение списка объектов бакета. Отсутствие бакета, файла или каталога, отсутствие файлов, соответствующих шаблону, и ошибки доступа прерывают создание таблицы. Ошибка содержит путь и адрес бакета.

Пустой бакет допустим при `LOCATION="/"`. Пустой каталог допустим, если в S3 есть объект-маркер с ключом, заканчивающимся на `/`. Префикс без объектов и маркеров в S3 не существует. Проверяется только `LOCATION`, независимо от `FILE_PATTERN`, проекции партиций и содержимого файлов.

По умолчанию проверка отключена. Не указывайте `VALIDATE_LOCATION` или задайте `"false"`, если таблица создается до появления данных, например для записи в новый префикс. Проверка при создании не гарантирует дальнейшую доступность пути.

Допускается использование только ограниченного подмножества типов данных:

- `Bool`.
- `Int8`, `Uint8`, `Int16`, `Uint16`, `Int32`, `Uint32`, `Int64`, `Uint64`.
- `Float`, `Double`.
- `Date`, `DateTime`.
- `String`, `Utf8`.

Без дополнительных модификаторов колонка приобретает [опциональный тип](../types/optional.md) тип, и допускает запись `NULL` в качестве значений. Для получения неопционального типа необходимо использовать `NOT NULL`.

## Пример

Cледующий SQL-запрос создает внешнюю таблицу с именем `s3_test_data`, в котором расположены файлы в формате `CSV` со строковыми полями `key` и `value`, находящиеся внутри бакета по пути `test_folder`, при этом для указания реквизитов подключения используется объект [подключение](../../../concepts/datamodel/external_data_source.md) `bucket`:

```yql
CREATE EXTERNAL TABLE s3_test_data (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder/",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```




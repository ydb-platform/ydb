# Запись данных в бакеты S3 ({{ objstorage-full-name }})

В {{ ydb-full-name }} для записи данных в бакет {{ objstorage-full-name }} можно использовать [внешние источники данных](#external-source-write) или [внешние таблицы](#external-table-write).

## Запись в S3 через внешний источник данных {#external-source-write}

Запись данных с помощью [внешних источников данных](../../datamodel/external_data_source.md) удобно использовать для прототипирования и первоначальной настройки работы с записью данных. SQL-выражение иллюстрирует запись данных во внешний источник данных напрямую.

```yql
INSERT INTO `external_source`.`test/`
WITH
(
    FORMAT = "csv_with_names"
)
SELECT
    "value" AS value, "name" AS name
```

Запись будет произведена в указанный путь, создаваемые при записи файлы получают случайные имена. В общем виде запрос записи в S3 ({{ objstorage-full-name }}) с использованием [внешнего источника данных](../../datamodel/external_data_source.md) выглядит следующим образом:

```yql
INSERT INTO
    <object_storage_external_source_name>.`<write_folder>`
WITH
(
    FORMAT = "<file_format>",
    COMPRESSION = "<compression>",
    SCHEMA = (<schema_definition>),
    <format_settings>
)
    <expression>
```

Где:

* `object_storage_external_source_name` — название внешнего источника данных, ведущего на бакет с S3 ({{ objstorage-full-name }}).
* `write_folder` — путь к фолдеру внутри бакета, в который будует вестись запись (не пустой путь, заканчивающийся на '/'), фолдер не должен быть корнем бакета.
* `file_format` — [формат данных](formats.md#formats) в файлах, обязательно.
* `compression` — опциональный [формат сжатия](formats.md#compression_formats) файлов.
* `schema_definition` — опциональное [описание схемы хранимых данных](external_data_source.md#schema) в файлах, если не указано, будет выведено автоматически из типа `<expression>`.
* `format_settings` — опциональные [параметры форматирования](external_data_source.md#format_settings)

Для задания партицирования данных нужно использовать `PARTITIONED_BY` в секции `WITH`, также как и при чтении из внешних источников данных, подробнее [здесь](partitioning.md#syntax-external-data-source), [расширенное партицирование](partition_projection.md) не поддержано для записи в S3. Партицирование данных при записи поддержано для всех [форматов записи](formats.md#formats), кроме `json_list` и `raw`.

При работе с внешними источниками данных и таблицами возможны только операции чтения (`SELECT`) и вставки (`INSERT`) данных, другие виды операций не поддерживаются.

## Запись данных через внешние таблицы {#external-table-write}

Если записывать данные нужно регулярно, то удобно делать это с помощью внешних таблиц. При этом нет необходимости указывать все детали работы с этими данными в каждом запросе. Для записи данных в бакет создайте [внешнюю таблицу](external_table.md) в S3 ({{ objstorage-full-name }}) и используйте обычное SQL-выражение `INSERT INTO`:

```yql
INSERT INTO `test`
SELECT
    "value" AS value, "name" AS name
```

## Экспорт данных в объектное хранилище S3 {#export-to-s3}

{{ ydb-short-name }} поддерживает экспорт данных из таблиц в S3 с помощью [внешнего источника данных](../../datamodel/external_data_source.md).

Для экспорта нужно использовать запрос [записи во внешний источник данных](#external-source-write) с указанием [формата экспортируемых файлов](./formats.md#formats):

```yql
INSERT INTO `external_source`.`test/`
WITH
(
    FORMAT = "parquet"
)
SELECT
    *
FROM table
```

Рекомендованным форматом экспорта является `parquet`, для него оптимизированы сценарии импорта и экспорта.

Экспорт из [колоночных таблиц](../../datamodel/table.md#column-oriented-tables) (OLAP-таблиц) поддерживается без ограничений. Экспорт из [строковых таблиц](../../datamodel/table.md#row-oriented-tables) поддерживается только для таблиц размером до 1 ГБ. Экспорт строковых таблиц без ограничения по размеру поддержан через YDB CLI, подробнее см. в статье [{#T}](../../../reference/ydb-cli/export-import/export-s3.md).

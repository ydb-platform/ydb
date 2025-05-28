# Экспорт и импорт данных между колоночными таблицами и S3

{{ ydb-short-name }} поддерживает удобный двусторонний обмен данными между колоночными таблицами базы данных и бакетами S3 ({{ objstorage-full-name }}). Это позволяет быстро переносить большие объемы данных для целей резервного копирования, загрузки внешних датасетов или миграции между разными хранилищами.

## Экспорт данных из колоночной таблицы в объектное хранилище S3 {#export-from-olap-to-s3}

Экспорт данных из [колоночных таблиц](../../datamodel/table.md#column-oriented-tables) (OLAP-таблиц) в {{ objstorage-full-name }} производится простым SQL-запросом, который записывает данные напрямую из таблицы в указанный бакет S3 с помощью [внешнего соединения](../../datamodel/external_data_source.md).

Для экспорта нужно использовать запрос [записи во внешнее соединение](write_data.md#connection-write):

```yql
INSERT INTO `external_source`.`folder/`
WITH
(
    FORMAT = "parquet"
)
SELECT
    *
FROM column_table
```

Схема для экспортируемых файлов Parquet автоматически формируется на основании схемы исходной колоночной таблицы. Если необходимо экспортировать данные с партиционированием полученных файлов, то используйте запись через [внешние таблицы](write_data.md#external-table-write).

## Импорт данных из S3 в колоночную таблицу {#import-from-s3-to-olap}

Загрузка данных из S3 в колоночную таблицу {{ ydb-short-name }} производится с помощью команды [CREATE TABLE ... AS SELECT](../../../yql/reference/syntax/create_table/index.md). В результате будет создана [колоночная таблица](../../datamodel/table.md#column-oriented-tables) со схемой, соответствующей использованной для импорта [внешней таблицы](../../datamodel/external_table.md):

```yql
CREATE TABLE column_table (
    PRIMARY KEY (key_column)
)
WITH (
    STORE = COLUMN
)
AS SELECT * FROM external_table
```

Колонки, указанные в `PRIMARY KEY`, должны быть помечены как `NOT NULL` во внешней таблице.

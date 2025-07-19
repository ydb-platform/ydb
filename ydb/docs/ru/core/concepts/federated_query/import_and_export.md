# Импорт и экспорт данных с использованием федеративных запросов

## Импорт данных {#import}

{{ ydb-full-name }} позволяет импортировать данные из любых внешних источников в таблицы базы данных. Для импорта данных нужно использовать запрос чтения из [внешнего источника данных](../datamodel/external_data_source.md) или [внешней таблицы](../datamodel/external_table.md) и записи в {{ ydb-short-name }} таблицу. Список поддерживаемых вариантов записи данных для строковых и колоночных таблиц:

|Операция|[Строковые таблицы](./datamodel/table.md#row-oriented-tables)|[Колоночные таблицы](../datamodel/table.md#column-oriented-tables)|
|--------|-----------------|------------------|
|[CREATE TABLE ... AS SELECT](../../yql/reference/syntax/create_table/index.md)|✓|✓|
|[UPSERT](../../yql/reference/syntax/upsert_into.md)||✓|
|[INSERT](../../yql/reference/syntax/insert_into.md)||✓|

{% note tip %}

Рекомендованными вариантами импорта с использованием федеративных запросов являются операции `CREATE TABLE ... AS SELECT` и `UPSERT`, для них значительно оптимизирован сценарий импорта данных.

{% endnote %}

Ниже приведён пример импорта данных из [внешней таблицы](../datamodel/external_table.md) указывающей на S3-совместимое хранилище в новою [колоночную таблицу](../datamodel/table.md#column-oriented-tables) с помощью команды [CREATE TABLE ... AS SELECT](../../../yql/reference/syntax/create_table/index.md). Параметры для создаваемой таблицы могут быть перечислены в секции `WITH`, подробнее см. в статье [{#T}](../../../yql/reference/syntax/create_table/with.md).

```yql
CREATE TABLE column_table (
    PRIMARY KEY (key)
)
WITH (
    STORE = COLUMN
)
AS SELECT * FROM s3_external_table
```

В результате будет создана [колоночная таблица](../datamodel/table.md#column-oriented-tables) со схемой, соответствующей использованной для импорта [внешней таблицы](../datamodel/external_table.md). Колонки, указанные в `PRIMARY KEY`, должны быть помечены как `NOT NULL` во внешней таблице.

Пример импорта данных из внешнего источника данных указывающего на [PostgreSQL](postgresql.md#query) в существующую таблицу:

```yql
UPSERT INTO target_table
SELECT * FROM postgresql_datasource.source_table
```

Подробнее про создание внешних источников данных и внешних таблиц, а так же запросы чтения данных из них с использованием федеративных запросов можно прочитать в статьях:

- [ClickHouse](clickhouse.md#query)
- [Greenplum](greenplum.md#query)
- [MySQL](mysql.md#query)
- [PostgreSQL](postgresql.md#query)
- [S3](s3/external_table.md)
- [{{ ydb-short-name }}](ydb.md#query)

## Экспорт данных {#export}

На данный момент экспорт данных с использованием федеративных запросов поддержан только для S3-совместимых хранилищ, подробнее смотреть в статье [{#T}](../../../reference/ydb-cli/export-import/export-s3.md).

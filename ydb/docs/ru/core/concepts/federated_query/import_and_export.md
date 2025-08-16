# Импорт и экспорт данных с использованием федеративных запросов

## Импорт данных {#import}

{{ ydb-full-name }} позволяет импортировать данные из любых внешних источников в таблицы базы данных. Для импорта данных нужно использовать запрос чтения из [внешнего источника данных](../datamodel/external_data_source.md) или [внешней таблицы](../datamodel/external_table.md) и записи в {{ ydb-short-name }} таблицу. Список поддерживаемых вариантов записи данных для строковых и колоночных таблиц, поддерживающих массивно-параллельный импорт неограниченного объёма данных:

|Операция|[Строковые таблицы](../datamodel/table.md#row-oriented-tables)|[Колоночные таблицы](../datamodel/table.md#column-oriented-tables)|
|--------|-----------------|------------------|
|[UPSERT](../../yql/reference/syntax/upsert_into.md)||✓|
|[INSERT](../../yql/reference/syntax/insert_into.md)||✓|

{% note tip %}

Рекомендованным вариантом импорта с использованием федеративных запросов является операция `UPSERT` — для неё значительно оптимизирован сценарий импорта данных.

{% endnote %}

Пример импорта данных из PostgreSQL таблицы в {{ ydb-short-name }} таблицу:

```yql
UPSERT INTO target_table
SELECT * FROM postgresql_datasource.source_table
```

Подробнее про создание внешних источников данных и внешних таблиц, а также про запросы чтения данных из них можно прочитать в статьях:

- [ClickHouse](clickhouse.md#query)
- [Greenplum](greenplum.md#query)
- [MySQL](mysql.md#query)
- [PostgreSQL](postgresql.md#query)
- [S3](s3/external_table.md)
- [{{ ydb-short-name }}](ydb.md#query)

## Экспорт данных {#export}

На данный момент экспорт данных с использованием федеративных запросов поддержан только для S3-совместимых хранилищ, подробнее смотреть в статье [{#T}](s3/write_data.md#export-to-s3).

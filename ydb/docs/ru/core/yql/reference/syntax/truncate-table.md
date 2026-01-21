
# TRUNCATE TABLE

Удаляет все пользовательские данные из указанной таблицы и ее индексов.

## Синтаксис

```yql
TRUNCATE TABLE <table_name>;
```

{% note info %}

* Во время выполнения ```TRUNCATE TABLE``` таблица блокируется для операций чтения и записи.
* Операцию нельзя прервать или отменить после начала выполнения.
* Операцию нельзя выполнить, если у таблицы есть:
    - [асинхронный вторичный индекс](../../../concepts/secondary_indexes.md#async),
    - добавленный [поток изменений](alter_table/changefeed.md),
    - [асинхронная репликация](../../../concepts/async-replication.md).

{% endnote %}

## Примеры

```yql
TRUNCATE TABLE `/Root/test/my_table`;
```

```yql
TRUNCATE TABLE my_table;
```

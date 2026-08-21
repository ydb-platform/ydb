# TRUNCATE TABLE

`TRUNCATE TABLE` удаляет все пользовательские данные из указанной таблицы и ее индексов.

## Синтаксис

```yql
TRUNCATE TABLE <table_name>;
```

## Ограничения

* Во время выполнения операции таблица блокируется на чтение и запись.
* Операцию нельзя прервать или отменить после начала выполнения.
* Операцию нельзя выполнить, если у таблицы есть:
    - [асинхронный вторичный индекс](../../../concepts/query_execution/secondary_indexes.md#async),
    - [поток изменений](alter_table/changefeed.md),
    - [асинхронная репликация](../../../concepts/async-replication.md).

## Примеры

Удаляет все данные из таблицы с полным путем `/Root/test/my_table`.

```yql
TRUNCATE TABLE `/Root/test/my_table`;
```

Удаляет все данные из таблицы `my_table` в текущей базе данных.

```yql
TRUNCATE TABLE `my_table`;
```


## См. также

* [CREATE TABLE](./create_table/index.md)
* [ALTER TABLE](./alter_table/index.md)
* [DROP TABLE](./drop_table.md)

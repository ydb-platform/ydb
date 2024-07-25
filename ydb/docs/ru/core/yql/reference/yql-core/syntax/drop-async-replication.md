# DROP ASYNC REPLICATION

Вызов `DROP ASYNC REPLICATION` удаляет объект [асинхронной репликации](../../../concepts/async-replication.md).

## Синтаксис {#syntax}

```sql
DROP ASYNC REPLICATION <name> [CASCADE]
```

где:
* `name` — имя объекта асинхронной репликации.
* `CASCADE` — каскадное удаление асинхронной репликации и объектов-реплик, созданных в рамках неё.

## Примеры {#examples}

Рассмотрим примеры удаления объекта асинхронной репликации, созданной следующим запросом:

```sql
CREATE ASYNC REPLICATION `my_replication`
FOR `table` AS `replica_table`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Удаление объекта асинхронной репликации, таблица `replica_table` остается:

```sql
DROP ASYNC REPLICATION `my_replication`;
```

Удаление объекта асинхронной репликации вместе с таблицей `replica_table`:

```sql
DROP ASYNC REPLICATION `my_replication` CASCADE;
```

## См. также

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [ALTER ASYNC REPLICATION](alter-async-replication.md)

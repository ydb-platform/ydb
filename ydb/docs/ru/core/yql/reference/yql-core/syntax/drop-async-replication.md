# DROP ASYNC REPLICATION

Вызов `DROP ASYNC REPLICATION` удаляет экземпляр [асинхронной репликации](../../../concepts/async-replication.md). Вместе с экземпляром асинхронной репликации [удаляются](../../../concepts/async-replication.md#drop):
* автоматически созданные потоки изменений;
* объекты-реплики (опционально).

## Синтаксис {#syntax}

```sql
DROP ASYNC REPLICATION <name> [CASCADE]
```

где:
* `name` — имя экземпляра асинхронной репликации.
* `CASCADE` — каскадное удаление объектов-реплик, созданных в рамках данного экземпляра асинхронной репликации.

## Примеры {#examples}

Рассмотрим примеры удаления экземпляра асинхронной репликации, созданной следующим запросом:

```sql
CREATE ASYNC REPLICATION `my_replication`
FOR `table` AS `replica_table`
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Удаление экземпляра асинхронной репликации и автоматически созданного потока изменений в таблице `table`, таблица `replica_table` остается:

```sql
DROP ASYNC REPLICATION `my_replication`;
```

Удаление экземпляра асинхронной репликации, автоматически созданного потока изменений в таблице `table` и таблицы `replica_table`:

```sql
DROP ASYNC REPLICATION `my_replication` CASCADE;
```

## См. также

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [ALTER ASYNC REPLICATION](alter-async-replication.md)

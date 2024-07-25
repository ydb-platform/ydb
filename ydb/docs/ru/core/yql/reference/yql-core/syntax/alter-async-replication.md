# ALTER ASYNC REPLICATION

Вызов `ALTER ASYNC REPLICATION` изменяет параметры и состояние экземпляра [асинхронной репликации](../../../concepts/async-replication.md).

## Синтаксис {#syntax}

```sql
ALTER ASYNC REPLICATION <name> SET (option = value [, ...])
```

где:
* `name` — имя экземпляра асинхронной репликации.
* `SET (option = value [, ...])` — [параметры](#params) асинхронной репликации.

### Параметры {#params}

* `STATE` — состояние асинхронной репликации. Возможные значения:
  * `DONE` — [завершение процесса асинхронной репликации](../../../concepts/async-replication.md#done).
* `FAILOVER_MODE` — режим переключения состояния. Применимо только совместно с параметром `STATE`. Возможные значения:
  * `FORCE` — принудительное переключение состояния.

## Примеры {#examples}

Следующий запрос принудительно завершит процесс асинхронной репликации:

```sql
ALTER ASYNC REPLICATION `my_replication` SET (STATE = "DONE", FAILOVER_MODE = "FORCE");
```

## См. также

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)

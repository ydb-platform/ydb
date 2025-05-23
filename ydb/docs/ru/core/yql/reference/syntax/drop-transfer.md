# DROP TRANSFER

Вызов `DROP TRANSFER` удаляет экземпляр [трансфера](../../../concepts/transfer.md). Вместе с экземпляром трансфера удалится автоматически созданный [consumer](../../../concepts/topic.md#changefeed);

## Синтаксис {#syntax}

```yql
DROP TRANSFER <name>
```

где:

* `name` — имя экземпляра асинхронной репликации.

## См. также

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)

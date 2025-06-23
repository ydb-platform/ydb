# DROP TRANSFER

Вызов `DROP TRANSFER` удаляет экземпляр [трансфера](../../../concepts/transfer.md). Вместе с экземпляром трансфера удалится автоматически созданный [consumer](../../../concepts/topic.md#changefeed).

Вызов `DROP TRANSFER` не удаляет экземпляр таблицы, в которую записываются данные, и экземпляр топика, из которого данные читаются.

## Синтаксис {#syntax}

```yql
DROP TRANSFER <name>
```

где:

* `name` — имя экземпляра трансфера.

## См. также

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)

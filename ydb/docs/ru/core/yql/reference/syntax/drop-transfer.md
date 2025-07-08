# DROP TRANSFER

Вызов `DROP TRANSFER` удаляет экземпляр [трансфера](../../../concepts/transfer.md). Вместе с экземпляром трансфера удалится и [читатель](../../../concepts/topic.md#changefeed), если он был создан автоматически при создании трансфера.

Вызов `DROP TRANSFER` не удаляет таблицу, в которую записываются данные, и топик, из которого данные читаются.

## Синтаксис {#syntax}

```yql
DROP TRANSFER <name>
```

где:

* `name` — имя экземпляра трансфера.

## См. также

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)

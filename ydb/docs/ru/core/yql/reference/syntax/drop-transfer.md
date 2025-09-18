# DROP TRANSFER

Вызов `DROP TRANSFER` удаляет экземпляр [трансфера](../../../concepts/transfer.md). Вместе с экземпляром трансфера удалится и [читатель](../../../concepts/datamodel/topic.md#consumer), если он был создан автоматически при создании трансфера. Попытки удаления читателя будут продолжаться до его успешного удаления.

Вызов `DROP TRANSFER` не удаляет таблицу, в которую записываются данные, и топик, из которого данные читаются.

## Синтаксис {#syntax}

```yql
DROP TRANSFER <name>
```

где:

* `name` — имя экземпляра трансфера.

## Разрешения

Для удаления трансфера требуются следующие [права](grant.md#permissions-list):

* `REMOVE SCHEMA` — для удаления экземпляра трансфера;
* `ALTER SCHEMA` — для удаления автоматически созданного читателя топика (если применимо).

## Примеры {#examples}

Следующий запрос удаляет трансфер c именем `my_transfer`:

```yql
DROP TRANSFER my_transfer;
```

## См. также

* [CREATE TRANSFER](create-transfer.md)
* [ALTER TRANSFER](alter-transfer.md)
* [{#T}](../../../concepts/transfer.md)

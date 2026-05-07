# DROP STREAMING QUERY

`DROP STREAMING QUERY` удаляет [потоковый запрос](../../../concepts/streaming-query.md).

## Синтаксис

```sql
DROP STREAMING QUERY [IF EXISTS] <query_name>
```

### Параметры

* `IF EXISTS` — не выводить ошибку, если потокового запроса не существует.
* `query_name` — имя потокового запроса, подлежащего удалению.

## Разрешения

Требуется [разрешение](./grant.md#permissions-list) `REMOVE SCHEMA` на потоковый запрос, пример выдачи такого разрешения для запроса `my_streaming_query`:

```sql
GRANT REMOVE SCHEMA ON my_streaming_query TO `user@domain`
```

## Примеры

Удаление запроса `my_streaming_query`:

```sql
DROP STREAMING QUERY my_streaming_query
```

## См. также

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](alter-streaming-query.md)

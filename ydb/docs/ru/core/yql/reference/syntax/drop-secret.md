# DROP SECRET

Команда `DROP SECRET` удаляет существующий [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
DROP SECRET secret_name
```

* `secret_name` — имя удаляемого секрета.

## Разрешения

Для удаления секрета требуются [права](grant.md#permissions-list) `REMOVE SCHEMA` и `ALTER SCHEMA`.

## Примеры

Удалить секрет с именем `secret_name`:

```sql
DROP SECRET secret_name;
```

## См. также

* [CREATE SECRET](create-secret.md)
* [ALTER SECRET](alter-secret.md)
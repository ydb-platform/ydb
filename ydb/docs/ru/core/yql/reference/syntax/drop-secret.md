# DROP SECRET

Команда `DROP SECRET` удаляет существующий [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
DROP SECRET [IF EXISTS] secret_name
```

* `IF EXISTS` — команда не возвращает ошибку, если секрет не существует; в этом случае она ничего не делает.
* `secret_name` — имя удаляемого секрета.

## Разрешения

Для удаления секрета требуются [права](grant.md#permissions-list) `REMOVE SCHEMA` и `ALTER SCHEMA`.

## Примеры

Удалить секрет с именем `secret_name`:

```sql
DROP SECRET secret_name;
```

Удалить секрет с именем `secret_name`, только если он существует; если он не существует, команда ничего не делает:

```sql
DROP SECRET IF EXISTS secret_name;
```

## См. также

* [CREATE SECRET](create-secret.md)
* [ALTER SECRET](alter-secret.md)
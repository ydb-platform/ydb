# DROP SECRET

Команда `DROP SECRET` удаляет существующий [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
DROP SECRET [IF EXISTS] secret_name
```

* `IF EXISTS` — команда не возвращает ошибку, если секрет не существует; в этом случае она ничего не делает.
* `secret_name` — имя удаляемого секрета.

Для [секрета с делегированием IAM](../../../concepts/datamodel/iam-delegation-secrets.md) после удаления объекта {{ ydb-short-name }} просит IAM отозвать делегирование: получить новые токены сервисного аккаунта через этот секрет больше нельзя. Если отзыв не удался, секрет всё равно удаляется, а команда завершается с предупреждением, содержащим идентификатор делегирования.

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
* [{#T}](../../../concepts/datamodel/iam-delegation-secrets.md)
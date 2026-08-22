# ALTER SECRET

Команда `ALTER SECRET` изменяет существующий [секрет](../../../concepts/datamodel/secrets.md).

Синтаксис:

```sql
ALTER SECRET [IF EXISTS] secret_name
WITH (option = value[, ...])
```

* `IF EXISTS` — команда не возвращает ошибку, если секрет не существует; в этом случае она ничего не делает.
* `secret_name` — имя изменяемого секрета.
* `option` — опция команды:
  * `value` — строка со значением секрета.

## Разрешения

Для изменения секрета требуется [право](grant.md#permissions-list) `ALTER SCHEMA`.

## Примеры

Изменить значение секрета `secret_name` на `secret_value_new`:

```sql
ALTER SECRET secret_name WITH (value = "secret_value_new");
```

Изменить значение секрета `secret_name` на `secret_value_new`, только если он существует; если он не существует, команда ничего не делает:

```sql
ALTER SECRET IF EXISTS secret_name WITH (value = "secret_value_new");
```

## См. также

* [CREATE SECRET](create-secret.md)
* [DROP SECRET](drop-secret.md)

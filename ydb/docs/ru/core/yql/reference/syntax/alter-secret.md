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
  * `SERVICE_ACCOUNT_ID`, `RESOURCE` — только для [секрета с делегированием IAM](../../../concepts/datamodel/iam-delegation-secrets.md): новый сервисный аккаунт и/или облако. Новое делегирование настраивается в IAM до отзыва старого, поэтому запросы, использующие секрет, переключаются на новый сервисный аккаунт без перезапуска. Источник секрета (`SOURCE`) изменить нельзя.

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

Сменить сервисный аккаунт [секрета с делегированием IAM](../../../concepts/datamodel/iam-delegation-secrets.md):

```sql
ALTER SECRET events_sa WITH (SERVICE_ACCOUNT_ID="aje6h0sbq9wl2xn5tdg7");
```

## См. также

* [CREATE SECRET](create-secret.md)
* [DROP SECRET](drop-secret.md)
* [{#T}](../../../concepts/datamodel/iam-delegation-secrets.md)

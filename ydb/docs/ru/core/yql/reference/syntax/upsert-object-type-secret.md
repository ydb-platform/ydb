# UPSERT OBJECT (TYPE SECRET)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Для изменения содержимого [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```yql
UPSERT OBJECT `secret_name` (TYPE SECRET) WITH value = `secret_value`;
```

Где:

* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

## Пример

Следующий запрос устанавливает новое значение секрета с именем `MySecretName` в значение `MySecretData`.

```yql
UPSERT OBJECT `MySecretName` (TYPE SECRET) WITH value = `MySecretData`;
```

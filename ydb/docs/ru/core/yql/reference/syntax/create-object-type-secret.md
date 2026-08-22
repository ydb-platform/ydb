# CREATE OBJECT (TYPE SECRET)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Для создания [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```yql
CREATE OBJECT <secret_name> (TYPE SECRET) WITH value = "<secret_value>";
```

Где:

* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

## Пример

Следующий запрос создает секрет с именем `MySecretName` и значением `MySecretData`.

```yql
CREATE OBJECT MySecretName (TYPE SECRET) WITH value = "MySecretData";
```

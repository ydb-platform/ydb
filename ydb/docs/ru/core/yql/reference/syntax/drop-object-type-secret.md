# DROP OBJECT (TYPE SECRET)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Удаляет указанный [секрет](../../../concepts/datamodel/secrets.md).

Если секрета с таким именем не существует, возвращается ошибка.

## Пример

```yql
DROP OBJECT my_secret (TYPE SECRET);
```


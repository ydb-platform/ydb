# DROP OBJECT (TYPE SECRET_ACCESS)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Удаляет указанное правило доступа к [секрету](../../../concepts/datamodel/secrets.md#secret_access).

Если правила с таким именем не существует, возвращается ошибка.

## Пример

```yql
DROP OBJECT (TYPE SECRET_ACCESS) `MySecretName:another_user`;
```

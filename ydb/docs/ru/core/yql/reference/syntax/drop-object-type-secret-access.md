# DROP OBJECT (TYPE SECRET_ACCESS)

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан в разделе [Секреты](../../../concepts/datamodel/secrets.md#secret_access).

{% endnote %}

Удаляет указанное правило доступа к [секрету](../../../concepts/datamodel/secrets.md#secret_access).

Если правила с таким именем не существует, возвращается ошибка.

## Пример

```yql
DROP OBJECT (TYPE SECRET_ACCESS) `MySecretName:another_user`;
```


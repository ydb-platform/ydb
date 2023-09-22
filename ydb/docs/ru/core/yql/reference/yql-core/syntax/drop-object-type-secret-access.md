# DROP OBJECT (TYPE SECRET_ACCESS)

{% note warning %}

Текущий систаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}


Удаляет указанное правило доступа к [секрету](../../../concepts/datamodel/secrets.md#secret_access).

Если правила с таким именем не существует, возвращается ошибка.

**Пример**

```yql
DROP OBJECT (TYPE SECRET_ACCESS) `MySecretName:another_user`;
```


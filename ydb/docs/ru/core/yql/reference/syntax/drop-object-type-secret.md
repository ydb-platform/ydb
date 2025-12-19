# DROP OBJECT (TYPE SECRET)

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан в разделе [Секреты](../../../concepts/datamodel/secrets.md#create_secret).

{% endnote %}

Удаляет указанный [секрет](../../../concepts/datamodel/secrets.md).

Если секрета с таким именем не существует, возвращается ошибка.

## Пример

```yql
DROP OBJECT my_secret (TYPE SECRET);
```


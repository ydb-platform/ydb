# UPSERT OBJECT (TYPE SECRET)

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан в разделе [Секреты](../../../concepts/datamodel/secrets.md#create_secret).

{% endnote %}

Для изменения содержимого [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```yql
UPSERT OBJECT `secret_name` (TYPE SECRET) WITH value=`secret_value`;
```

Где:

* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

## Пример

Следующий запрос устанавливает новое значение секрета с именем `MySecretName` в значение `MySecretData`.

```yql
UPSERT OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```

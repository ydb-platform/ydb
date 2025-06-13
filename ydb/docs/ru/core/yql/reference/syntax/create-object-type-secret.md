# CREATE OBJECT (TYPE SECRET)

{% note warning %}

Текущий синтаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}

Для создания [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```yql
CREATE OBJECT <secret_name> (TYPE SECRET) WITH value="<secret_value>";
```

Где:

* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

## Пример

Следующий запрос создает секрет с именем `MySecretName` и значением `MySecretData`.

```yql
CREATE OBJECT MySecretName (TYPE SECRET) WITH value="MySecretData";
```

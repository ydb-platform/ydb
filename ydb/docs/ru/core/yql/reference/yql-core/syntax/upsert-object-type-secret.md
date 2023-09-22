# UPSERT OBJECT (TYPE SECRET)

{% note warning %}

Текущий систаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}


Для изменения содержимого [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```sql
UPSERT OBJECT `secret_name` (TYPE SECRET) WITH value=`secret_value`;
```
Где:
* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

**Пример**

Следующий запрос устанавливает новое значение секрета с именем `MySecretName` в значение `MySecretData`.

```sql
UPSERT OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```

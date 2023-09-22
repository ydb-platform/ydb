# CREATE OBJECT (TYPE SECRET)

{% note warning %}

Текущий систаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}


Для создания [секрета](../../../concepts/datamodel/secrets.md) используется следующий SQL-запрос:

```sql
CREATE OBJECT `secret_name` (TYPE SECRET) WITH value=`secret_value`;
```
Где:
* `secret_name` - имя секрета.
* `secret_value` - содержимое секрета.

**Пример**

Следующий запрос создает секрет с именем `MySecretName` и значением `MySecretData`.

```sql
CREATE OBJECT `MySecretName` (TYPE SECRET) WITH value=`MySecretData`;
```

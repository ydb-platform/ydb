# CREATE OBJECT (TYPE SECRET_ACCESS)

{% note warning %}

Текущий систаксис работы с секретами является временным, в будущих релизах {{ydb-full-name}} он будет изменен.

{% endnote %}


Для управления доступом к секретам используются специальные объекты `SECRET_ACCESS`.

```sql
CREATE OBJECT `secret_name:user_name` (TYPE SECRET_ACCESS)
```

Где:
* `secret_name` - имя [секрета](../../../concepts/datamodel/secrets.md).
* `user_name` - имя пользователя, которому выдается доступ.

**Пример**

Следующий SQL-запрос выдаст права на использование секрета `MySecretName` пользователю `another_user`:

```sql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS)
```

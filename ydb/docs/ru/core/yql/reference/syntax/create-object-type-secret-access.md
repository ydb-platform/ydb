# CREATE OBJECT (TYPE SECRET_ACCESS)

{% note warning %}

**Данная команда устарела** и будет удалена в будущих версиях {{ydb-full-name}}. Рекомендуемый синтаксис работы с секретами описан в разделе [Секреты](../../../concepts/datamodel/secrets.md#secret_access).

{% endnote %}


Для управления доступом к секретам используются специальные объекты `SECRET_ACCESS`.

```yql
CREATE OBJECT `secret_name:user_name` (TYPE SECRET_ACCESS)
```

Где:

* `secret_name` - имя [секрета](../../../concepts/datamodel/secrets.md).
* `user_name` - имя пользователя, которому выдается доступ.

## Пример

Следующий SQL-запрос выдаст права на использование секрета `MySecretName` пользователю `another_user`:

```yql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS)
```

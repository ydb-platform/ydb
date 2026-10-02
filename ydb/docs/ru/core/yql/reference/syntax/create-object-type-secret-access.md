# CREATE OBJECT (TYPE SECRET_ACCESS)

{% include [deprecated_secrets_warning](./_includes/deprecated_secrets_warning.md) %}

Все права на использование секрета принадлежат создателю секрета. Создатель секрета может предоставить право чтения секрета другому пользователю с помощью управления доступом к секретам.

Для управления доступами к секретам используются специальные объекты `SECRET_ACCESS`. Для выдачи разрешения на использование секрета `secret_name` пользователю `user_name` необходимо создать объект `SECRET_ACCESS` с именем `secret_name:user_name`.

```yql
CREATE OBJECT `secret_name:user_name` (TYPE SECRET_ACCESS);
```

Где:

* `secret_name` - имя [секрета](create-object-type-secret.md).
* `user_name` - имя пользователя, которому выдается доступ.

## Пример

Следующий SQL-запрос выдаст права на использование секрета `MySecretName` пользователю `another_user`:

```yql
CREATE OBJECT `MySecretName:another_user` (TYPE SECRET_ACCESS);
```

# CREATE USER

Создает пользователя с указанным именем и паролем.

Синтаксис:

```yql
CREATE USER user_name [option]
```

* `user_name` — имя пользователя. Может содержать строчные буквы латинского алфавита и цифры.
* `option` — опция команды:
  * `PASSWORD 'password'` — создает пользователя с паролем `password`.
  * `PASSWORD NULL` — создает пользователя с пустым паролем.
  Значение по умолчанию: `PASSWORD NULL`.

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

# ALTER USER

Изменяет пароль пользователя.

Синтаксис:

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

* `user_name` — имя пользователя.
* `option` — опция команды:

  * `PASSWORD 'password'` — изменяет пароль на `password`.
  * `PASSWORD NULL` — устанавливает пустой пароль.
  * `NOLOGIN` - запрет на логин пользователя (блокировка пользователя).
  * `LOGIN` - разрешение на логин пользователя (разблокировка пользователя).

## Встроенные пользователи

{% include [!](../_includes/initial_groups_and_users.md) %}

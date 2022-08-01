# ALTER USER

Изменяет пароль пользователя.

Синтаксис:

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

* `user_name` — имя пользователя.
* `option` — пароль пользователя:
  * `PASSWORD 'password'` — создает пользователя с паролем `password`. Опция `ENCRYPTED` всегда включена.
  * `PASSWORD NULL` — создает пользователя с пустым паролем.

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

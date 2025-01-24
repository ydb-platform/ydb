# ALTER USER

Changes the user password.

## Syntax

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

* `user_name`: The name of the user.
* `option` — The command option:

  * `PASSWORD 'password'` — changes the password to `password`.
  * `PASSWORD NULL` — sets an empty password.
  * `NOLOGIN` - disallows user login (user lockout).
  * `LOGIN` - allows user login (user unlocking).

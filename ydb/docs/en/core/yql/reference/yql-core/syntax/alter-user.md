# ALTER USER

Changes the user password.

Syntax:

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

* `user_name`: The name of the user.
* `option`: The password of the user:

  * `PASSWORD 'password'` creates a user with the `password` password. The `ENCRYPTED` option is always enabled.
  * `PASSWORD NULL` creates a user with an empty password.

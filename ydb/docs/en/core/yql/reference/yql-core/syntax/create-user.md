# CREATE USER

Creates a user with the specified name and password.

Syntax:

```yql
CREATE USER user_name [option]
```

* `user_name`: The name of the user. It may contain lowercase Latin letters and digits.
* `option`: The password of the user:

  * `PASSWORD 'password'` creates a user with the `password` password. The `ENCRYPTED` option is always enabled.
  * `PASSWORD NULL` creates a user with an empty password.

# CREATE USER

Creates a user with the specified name and password.

Syntax:

```yql
CREATE USER user_name [option]
```

* `user_name`: The name of the user. It may contain lowercase Latin letters and digits.
* `option` — command option:
  * `PASSWORD 'password'` — creates a user with the password `password`.
  * `PASSWORD NULL` — creates a user with an empty password (default).
  * `NOLOGIN` - disallows user login (user lockout).
  * `LOGIN` - allows user login (default).

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

# CREATE USER

Creates a user with the specified name and password.

Syntax:

```yql
CREATE USER user_name [option]
```

* `user_name`: The name of the user. It may contain lowercase Latin letters and digits.
* `option` — command option:
  * `PASSWORD 'password'` — creates a user with the password `password`; you can't use it together with `HASH`.
  * `PASSWORD NULL` — creates a user with an empty password (default).
  * `NOLOGIN` - disallows user login (user lockout).
  * `LOGIN` - allows user login (default).
  * `HASH 'hash'` -  creates a user with the a password whose hash is equal to `hash`; you can't uset it together with `PASSWORD`.

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## Notes

The YDB stores the user's password in encrypted form. Therefore, in order to be able to restore the user during database backup, there is a `HASH` option that allows you to create a user knowing only the hash in JSON format.

In the `HASH` option, the 'hash' parameter must get a JSON object with exactly three fields:

* `hash` - value of hash in base64 format;
* `salt` - sault in base64 format;
* `type` - hashing algorithm; this value always must be equal `argon2id`.

There is example:

```yql
CREATE USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}'
```

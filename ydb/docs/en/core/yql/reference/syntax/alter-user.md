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
  * `HASH 'hash'` -  creates a user with the a password whose hash is equal to `hash`; you can't uset it together with `PASSWORD`.

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## Notes

### Hash

The YDB stores the user's password in encrypted form. Therefore, in order to be able to restore the user during database backup, there is a `HASH` option that allows you to alter a user, after his creating, knowing only the hash in JSON format.

In the `HASH` option, the 'hash' parameter must get a JSON object with exactly three fields:

* `hash` - value of hash in base64 format;
* `salt` - sault in base64 format;
* `type` - hashing algorithm; this value always must be equal `argon2id`.

There is example:

```yql
ALTER USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}'
```

### Login

Only for [local users](../../../concepts/glossary.md#access-user).

If the limit on the number of authorization attempts is exceeded, the user is temporarily blocked. To unlock user ahead of time, you can use the `LOGIN` option.

There is example:

```yql
ALTER USER user1 LOGIN
```

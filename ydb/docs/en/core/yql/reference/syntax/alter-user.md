# ALTER USER

Changes the database user.

## Syntax

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

* `user_name` - The name of the user.
* `option` — The command option:
  * `PASSWORD 'password'` — changes the password to `password`; you can't use it together with `HASH`.
  * `PASSWORD NULL` — sets an empty password; you can't use it together with `HASH`.
  * `HASH 'hash'` -  sets the user's password, which will have a hash is equal to `hash`; you can't use it together with `PASSWORD`.
  * `NOLOGIN` - disallows user to log in; you can't use it together with `LOGIN`.
  * `LOGIN` - allows user to log in; you can't use it together with `NOLOGIN`.

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

Database administrator can change the user's password. Note, that password should be in quotation marks, except in case with `PASSWORD NULL`.

{% note info %}

User can change his password, even if he is not administrator.

{% endnote %}

There are examples:

```yql
ALTER USER user1 PASSWORD 'password';
```

```yql
ALTER USER user1 PASSWORD NULL;
```

## HASH

The {{ ydb-short-name }} stores the user's password in hashed form. Therefore, in order to be able to restore the user with same password during database backup, there is a `HASH` option that allows you to alter a user, after his creating, knowing only the hash in JSON format.

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

## NOLOGIN

Database administrator can block user.

There is example:

```yql
ALTER USER user1 NOLOGIN;
```

## LOGIN

If the limit on the number of authorization attempts is exceeded, the user is temporarily blocked. To unlock user ahead of time, you can use the `LOGIN` option.

Besides, user can be blocked by `NOLOGIN` option. In this case, database administrator can unblock user by `LOGIN` option.

There is example:

```yql
ALTER USER user1 LOGIN;
```

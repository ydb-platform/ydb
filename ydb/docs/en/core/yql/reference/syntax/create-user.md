# CREATE USER

Creates the database user.

Syntax:

```yql
CREATE USER user_name [option]
```

* `user_name` - The name of the user. It may contain lowercase Latin letters and digits.
* `option` — command option:
  * `PASSWORD 'password'` — creates a user with the password `password`; you can't use it together with `HASH`.
  * `PASSWORD NULL` — creates a user with an empty password; you can't use it together with `HASH`; default value.
  * `HASH 'hash'` - creates a user with the a password whose hash is equal to `hash`; you can't use it together with `PASSWORD`.
  * `NOLOGIN` - disallows user to log in; you can't use it together with `LOGIN`.
  * `LOGIN` - allows user to log in; you can't use it together with `NOLOGIN`; default value.

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

The database administrator can set the user's password when creating it. Note, that password should be in quotation marks, except in case with `PASSWORD NULL`.

There are examples:

```yql
CREATE USER user1 PASSWORD 'password';
```

```yql
CREATE USER user1 PASSWORD NULL;
```

## HASH

The {{ ydb-short-name }} stores the user's password in hashed form. Therefore, in order to be able to restore the user with same password during database backup, there is a `HASH` option that allows you to create a user knowing only the hash in JSON format.

In the `HASH` option, the 'hash' parameter must get a JSON object with exactly three fields:

* `hash` - value of hash in base64 format;
* `salt` - [salt](https://en.wikipedia.org/wiki/Salt_(cryptography)) in base64 format;
* `type` - hashing algorithm; this value always must be equal `argon2id`.

There is example:

```yql
CREATE USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}';
```

## NOLOGIN

Database administrator can create blocked user. Blocked user can't log in in system.

There is example:

```yql
CREATE USER user1 NOLOGIN;
```

## LOGIN

The option explicitly indicates that the user is being created unblocked. By default (that is, without specifying the `LOGIN` and `NOLOGIN` options), created user is not blocked.

There is example:

```yql
CREATE USER user1 LOGIN;
```

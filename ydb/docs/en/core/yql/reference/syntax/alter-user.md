# ALTER USER

Alters a database user.

## Syntax

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

{% include [!](../../../_includes/user-options.md) %}

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

Users can change only their own passwords. Database administrators can change a password of any user.

If the option is not `PASSWORD NULL`, enclose password in quotation marks.

Examples:

```yql
ALTER USER user1 PASSWORD 'password';
```

```yql
ALTER USER user1 PASSWORD "password";
```

```yql
ALTER USER user1 PASSWORD NULL;
```

## HASH

{% include [!](../../../_includes/hash-option.md) %}

So, the `HASH` option must get a JSON object with the following three fields:

* `hash` — hash value in base64 format;
* `salt` — [salt](https://en.wikipedia.org/wiki/Salt_(cryptography)) in base64 format;
* `type` – hashing algorithm (always set it to `argon2id`)

Example:

```yql
ALTER USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}'
```

## NOLOGIN

A database administrator can block a user from logging in to the database.

Example:

```yql
ALTER USER user1 NOLOGIN;
```

## LOGIN

If the limit on the number of authentication attempts is exceeded, the user is temporarily blocked. To unlock the user before the lockout period ends, you can use the `LOGIN` option.

Besides, a user can be blocked by the `NOLOGIN` option. In this case, a database administrator can use the `LOGIN` option to unblock the user.

Example:

```yql
ALTER USER user1 LOGIN;
```

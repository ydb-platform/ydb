# CREATE USER

Creates a database user.

Syntax:

```yql
CREATE USER user_name [option]
```

{% include [!](../../../_includes/user-options.md) %}

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

The database administrator can set the user's password when creating it. If the option is not `PASSWORD NULL`, enclose password in quotation marks.

Examples:

```yql
CREATE USER user1 PASSWORD 'password';
```

```yql
CREATE USER user1 PASSWORD "password";
```

```yql
CREATE USER user1 PASSWORD NULL;
```

## HASH

{% include [!](../../../_includes/hash-option.md) %}

So, the `HASH` option must get a JSON object with the following three fields:

* `hash` — hash value in base64 format;
* `salt` — [salt](https://en.wikipedia.org/wiki/Salt_(cryptography)) in base64 format;
* `type` – hashing algorithm (always set it to `argon2id`).

Example:

```yql
CREATE USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}';
```

## NOLOGIN

Database administrator can create user blocked from logging in to the database.

Example:

```yql
CREATE USER user1 NOLOGIN;
```

## LOGIN

The option explicitly indicates that the created user is unblocked. If the `LOGIN` and `NOLOGIN` options are not specified, new users are not blocked.

Example:

```yql
CREATE USER user1 LOGIN;
```

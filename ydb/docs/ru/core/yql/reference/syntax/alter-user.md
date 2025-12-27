# ALTER USER

Изменяет пользователя базы данных.

Синтаксис:

```yql
ALTER USER user_name [ WITH ] option [ ... ]
```

{% include [!](../../../_includes/user-options.md) %}

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

Пользователь может поменять только свой пароль. Администратор базы данных может изменить или снять пароль пользователя.

Пароль должен задаваться в кавычках, если только не используется опция `PASSWORD NULL`.

Примеры:

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

Параметр `'hash'` должен принимать JSON-объект, в котором есть ровно три поля:

* `hash` — значение самого хеша в формате base64;
* `salt` — [соль](https://ru.wikipedia.org/wiki/Соль_(криптография)) в формате base64;
* `type` — алгоритм, которым был хеширован пароль; это значение должно быть всегда равно `argon2id`.


Пример:

```yql
ALTER USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}';
```

## NOLOGIN

Администратор базы данных может заблокировать пользователя. Блокировка пользователя означает, что в системе устанавливается запрет на вход пользователя в систему.

Пример:

```yql
ALTER USER user1 NOLOGIN;
```

## LOGIN

При превышении ограничения на количество попыток аутентификации пользователь временно блокируется. Администратор базы данных может снять блокировку досрочно, используя опцию `LOGIN`.

Кроме того, пользователь может быть заблокирован опцией `NOLOGIN`. В этом случае администратор базы данных так же может снять блокировку с помощью `LOGIN`.

Пример:

```yql
ALTER USER user1 LOGIN;
```

## Встроенные пользователи

{% include [!](../_includes/initial_groups_and_users.md) %}

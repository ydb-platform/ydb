# CREATE USER

Создает пользователя базы данных.

Синтаксис:

```yql
CREATE USER user_name [option]
```

{% include [!](../../../_includes/user-options.md) %}

{% include [!](../../../_includes/do-not-create-users-in-ldap.md) %}

## PASSWORD

Администратор базы данных может задать пароль пользователя при его создании. Пароль должен задаваться в кавычках, если только не используется опция `PASSWORD NULL`.

Примеры:

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

Такиим образом, параметр `'hash'` должен принимать JSON-объект, в котором есть ровно три поля:

* `hash` — значение самого хеша в формате base64;
* `salt` — [соль](https://ru.wikipedia.org/wiki/Соль_(криптография)) в формате base64;
* `type` — алгоритм, которым был хеширован пароль; это значение должно быть всегда равно `argon2id`.

Пример:

```yql
CREATE USER user1 HASH '{
    "hash": "p4ffeMugohqyBwyckYCK1TjJfz3LIHbKiGL+t+oEhzw=",
    "salt": "U+tzBtgo06EBQCjlARA6Jg==",
    "type": "argon2id"
}';
```

## NOLOGIN

Администратор базы данных может создать пользователя сразу в заблокированном состоянии. Блокировка пользователя означает, что в системе устанавливается запрет на вход пользователя в систему.

Пример:

```yql
CREATE USER user1 NOLOGIN;
```

## LOGIN

Опция явно указывает, что пользователь создаетсяне в заблокированном состоянии. Это поведение по умолчанию (когда не указана ни одна из опций `LOGIN` или `NOLOGIN`).

Пример:

```yql
CREATE USER user1 LOGIN;
```

## Встроенные пользователи

{% include [!](../_includes/initial_groups_and_users.md) %}

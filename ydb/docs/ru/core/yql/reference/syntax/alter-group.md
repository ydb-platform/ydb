# ALTER GROUP

Добавляет или удаляет группу указанному пользователю. Для одного оператора вы можете указать несколько пользователей.

## Синтаксис

```yql
ALTER GROUP role_name ADD USER user_name [, ... ]
ALTER GROUP role_name DROP USER user_name [, ... ]
```

* `role_name` — имя группы.
* `user_name` — имя пользователя.

## Встроенные группы

{% include [!](../_includes/initial_groups_and_users.md) %}

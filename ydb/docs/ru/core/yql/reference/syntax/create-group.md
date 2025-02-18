# CREATE GROUP

Создает [группу](../../../concepts/glossary.md#access-group) с указанным именем. Есть возможность указать список [пользователей](../../../concepts/glossary.md#access-user), входящих в эту группу.

## Синтаксис

```yql
CREATE GROUP group_name [ WITH USER user_name [ , user_name [ ... ]] [ , ] ]
```

### Параметры

* `group_name` — имя группы. Может содержать строчные буквы латинского алфавита и цифры.
* `user_name` — имя пользователя, который станет участником группы после её создания. Может содержать строчные буквы латинского алфавита и цифры.

## Примеры

```yql
CREATE GROUP group1;
```

```yql
CREATE GROUP group2 WITH USER user1;
```

```yql
CREATE GROUP group3 WITH USER user1, user2,;
```

```yql
CREATE GROUP group4 WITH USER user1, user3, user2;
```

## Встроенные группы

{% include [!](../_includes/initial_groups_and_users.md) %}

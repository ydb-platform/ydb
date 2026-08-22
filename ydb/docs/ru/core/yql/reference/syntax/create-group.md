# CREATE GROUP

Команда `CREATE GROUP` создаёт [группу](../../../concepts/glossary.md#access-group) с указанным именем. Есть возможность указать список [пользователей](../../../concepts/glossary.md#access-user), входящих в эту группу.

Синтаксис:

```yql
CREATE GROUP group_name [ WITH USER user_name [ , user_name [ ... ]] [ , ] ]
```

* `group_name` — имя группы. Может содержать только строчные буквы латинского алфавита (a–z), цифры (0–9). Имя не должно быть пустым.
* `user_name` — имя пользователя, который станет участником группы после её создания. Может содержать строчные буквы латинского алфавита и цифры.

## Примеры

### Успешно выполненные команды

Создание группы:

```yql
CREATE GROUP group1;
```

```yql
CREATE GROUP group2 WITH USER root;
```

```yql
CREATE GROUP group3 WITH USER root, root1;
```

```yql
CREATE GROUP group4 WITH USER root, root1, root2;
```

### Обработка ошибок

Корректно обработанные ошибки:

| Ситуация | Сообщение об ошибке |
|----------|---------------------|
| Повторное создание группы | Group already exists |
| Невалидное имя (например, `group1№`) | token recognition error at: '№' |

## Встроенные группы

{% include [!](../_includes/initial_groups_and_users.md) %}

## См. также

* [ALTER GROUP](alter-group.md)
* [DROP GROUP](drop-group.md)

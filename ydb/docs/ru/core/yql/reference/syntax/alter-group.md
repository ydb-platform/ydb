# ALTER GROUP

Команда `ALTER GROUP` добавляет или удаляет пользователей в указанной [группе](../../../concepts/glossary.md#access-group). Для одного оператора вы можете указать несколько пользователей.

Синтаксис:

```yql
ALTER GROUP group_name ADD USER user_name [, ... ]
ALTER GROUP group_name DROP USER user_name [, ... ]
```

* `group_name` — имя группы. Может содержать только строчные буквы латинского алфавита (a–z), цифры (0–9). Имя не должно быть пустым.
* `user_name` — имя пользователя.

## Примеры

### Успешно выполненные команды

Добавление и удаление пользователей:

```yql
ALTER GROUP group1 ADD USER root;
```

```yql
ALTER GROUP group1 DROP USER root;
```

### Обработка ошибок

Корректно обработанные ошибки:

| Ситуация | Сообщение об ошибке |
|----------|---------------------|
| Обращение к несуществующей группе | Group not found |

## Встроенные группы

{% include [!](../_includes/initial_groups_and_users.md) %}

## См. также

* [CREATE GROUP](create-group.md)
* [DROP GROUP](drop-group.md)

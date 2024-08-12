# Управление доступом

{{ ydb-short-name }} поддерживает аутентификацию по логину паролю. Подробнее читайте в разделе [{#T}](../concepts/auth.md).

## Встроенные группы {#builtin}

Кластер {{ ydb-short-name }} имеет встроенные группы, предоставляющие заранее определенные наборы ролей:

Группа | Описание
--- | ---
`ADMINS` | Неограниченные права на всю схему кластера.
`DATABASE-ADMINS` | Права на создание и удаление баз данных (`CreateDatabase`, `DropDatabase`).
`ACCESS-ADMINS` | Права на управление правами других пользователей (`GrantAccessRights`).
`DDL-ADMINS` | Права на изменение схемы баз данных (`CreateDirectory`, `CreateTable`, `WriteAttributes`, `AlterSchema`, `RemoveSchema`).
`DATA-WRITERS` | Права на изменение данных (`UpdateRow`, `EraseRow`).
`DATA-READERS` | Права на чтение данных (`SelectRow`).
`METADATA-READERS` | Права на чтение метаинформации, без доступа к данным (`DescribeSchema` и `ReadAttributes`).
`USERS` | Права на подключение к базам данных (`ConnectDatabase`).

Все пользователи по умолчанию входят в группу `USERS`. Пользователь `root` по умолчанию входит в группу `ADMINS`.

Ниже показано, как группы наследуют разрешения друг друга. Например, в `DATA-WRITERS` входят все разрешения `DATA-READERS`:

![groups](../_assets/groups.svg)

## Управление группами {#groups}

Для создания, изменения или удаления группы воспользуйтесь операторами YQL:

* [{#T}](../yql/reference/syntax/create-group.md).
* [{#T}](../yql/reference/syntax/alter-group.md).
* [{#T}](../yql/reference/syntax/drop-group.md).

{% note info %}

Имена встроенных групп в команде `ALTER GROUP` необходимо указывать в верхнем регистре. Кроме того, при указании встроенных групп, содержащих в своем имени символ "-", необходимо использовать обратные кавычки (бэктики), например:

```
ALTER GROUP `DATA-WRITERS` ADD USER myuser1;
```

{% endnote %}

## Управление пользователями {#users}

Для создания, изменения или удаления пользователя воспользуйтесь операторами YQL:

* [{#T}](../yql/reference/syntax/create-user.md).
* [{#T}](../yql/reference/syntax/alter-user.md).
* [{#T}](../yql/reference/syntax/drop-user.md).

## Управление правами доступа операторами YQL

Для назначения или отзыва прав доступа на объекты схемы для пользователей или групп воспользуйтесь операторами YQL:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

# Управление доступом

{{ ydb-short-name }} поддерживает аутентификацию по логину паролю. Подробнее читайте в разделе [{#T}](authentication.md).

## Встроенные группы {#builtin}

## Управление группами {#groups}

Для создания, изменения или удаления группы воспользуйтесь операторами YQL:

* [{#T}](../yql/reference/syntax/create-group.md).
* [{#T}](../yql/reference/syntax/alter-group.md).
* [{#T}](../yql/reference/syntax/drop-group.md).

{% note info %}

Имена встроенных групп в команде `ALTER GROUP` необходимо указывать в верхнем регистре. Кроме того, при указании встроенных групп, содержащих в своем имени символ "-", необходимо использовать обратные кавычки (бэктики), например:

```yql
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

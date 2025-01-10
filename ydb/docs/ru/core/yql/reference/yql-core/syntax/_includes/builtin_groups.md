## Встроенные группы {#builtin}

Кластер {{ ydb-short-name }} имеет встроенные группы, предоставляющие заранее определенные наборы ролей:

Группа | Описание
--- | ---
`ADMINS` | Неограниченные права на всю схему кластера
`DATABASE-ADMINS` | Права на создание и удаление баз данных (`CreateDatabase`, `DropDatabase`)
`ACCESS-ADMINS` | Права на управление правами других пользователей (`GrantAccessRights`)
`DDL-ADMINS` | Права на изменение схемы баз данных (`CreateDirectory`, `CreateTable`, `WriteAttributes`, `AlterSchema`, `RemoveSchema`)
`DATA-WRITERS` | Права на изменение данных (`UpdateRow`, `EraseRow`)
`DATA-READERS` | Права на чтение данных (`SelectRow`)
`METADATA-READERS` | Права на чтение метаинформации, без доступа к данным (`DescribeSchema` и `ReadAttributes`)
`USERS` | Права на подключение к базам данных (`ConnectDatabase`)

Все пользователи по умолчанию входят в группу `USERS`. Пользователь `root` по умолчанию входит в группу `ADMINS`.

Ниже показано, как группы наследуют разрешения друг друга. Например, в `DATA-WRITERS` входят все разрешения `DATA-READERS`:

![builtin_groups](../_assets/builtin_groups.svg)

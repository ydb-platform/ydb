# Разрешения

## Общий перечень команд

Получить список доступных команд можно через интерактивную справку:

```bash
{{ ydb-cli }} scheme permissions --help
```

```text
Usage: ydb [global options...] scheme permissions [options...] <subcommand>

Description: Modify permissions

Subcommands:
permissions                 Modify permissions
├─ chown                    Change owner
├─ clear                    Clear permissions
├─ grant                    Grant permission (aliases: add)
├─ list                     List permissions
├─ revoke                   Revoke permission (aliases: remove)
├─ set                      Set permissions
├─ clear-inheritance        Set to do not inherit permissions from the parent
└─ set-inheritance          Set to inherit permissions from the parent
```

Все команды имеют дополнительный параметр, который для них не критичен:
`--timeout ms` - технологический параметр, задающий таймаут на отклик сервера.

## grant, revoke

Команды `grant` и `revoke` позволяют установить и отозвать, соответственно, права доступа к объектам схемы для пользователя или группы пользователей. По сути, это аналоги соответствующих YQL-команд [GRANT](../../../yql/reference/syntax/grant.md) и [REVOKE](../../../yql/reference/syntax/revoke.md).

Синтаксис команд {{ ydb-short-name }} CLI выглядит следующим образом:

```bash
{{ ydb-cli }} [connection options] scheme permissions grant  [options...] <path> <subject>
{{ ydb-cli }} [connection options] scheme permissions revoke [options...] <path> <subject>
```

Параметры:

`<path>` — полный путь от корня кластера до объекта, права на который необходимо модифицировать;
`<subject>` — имя пользователя или группы, для которых изменяются права доступа.

Дополнительные параметры `[options...]`:

`{-p|--permission} NAME` — список прав, которые необходимо предоставить (grant) или отозвать (revoke) у пользователя.

Каждое право нужно передавать отдельным параметром, например:

```bash
{{ ydb-cli }} scheme permissions grant -p "ydb.access.grant" -p "ydb.generic.read" '/Root/db1/MyApp/Orders' testuser
```

## set

Команда `set` позволяет установить права доступа к объектам схемы для пользователя или группы пользователей.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions set  [options...] <path> <subject>
```

Значения всех параметров полностью идентичны командам [`grant`, `revoke`](#grant-revoke). Однако ключевое отличие команды `set` от `grant` и `revoke` заключается в установке на указанный объект ровно тех прав доступа, которые перечислены в параметрах `-p (--permission)`. Другие права для указанного пользователя или группы будут отозваны.

Например, ранее пользователю `testuser` на объект `'/Root/db1'` были выданы права `"ydb.granular.select_row"`, `"ydb.granular.update_row"`, `"ydb.granular.erase_row"`, `"ydb.granular.read_attributes"`, `"ydb.granular.write_attributes"`, `"ydb.granular.create_directory"`.
Тогда в результате выполнения команды все права на указанный объект будут отозваны (как будто для каждого из прав вызывали `revoke`) и останется только право `"ydb.granular.select_row"` — непосредственно указанное в команде `set`:

```bash
{{ ydb-cli }} scheme permissions set -p "ydb.granular.select_row" '/Root/db1' testuser
```

## list

Команда `list` позволяет получить текущий список прав доступа к объектам схемы.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions list [options...] <path>
```

Параметры:
`<path>` — полный путь от корня кластера до объекта, права на который необходимо получить.

Пример результата выполнения `list`:

```bash
{{ ydb-cli }} scheme permissions list '/Root/db1/MyApp'
```

```bash
Owner: root

Permissions:
user1:ydb.generic.read

Effective permissions:
USERS:ydb.database.connect
METADATA-READERS:ydb.generic.list
DATA-READERS:ydb.granular.select_row
DATA-WRITERS:ydb.tables.modify
DDL-ADMINS:ydb.granular.create_directory,ydb.granular.write_attributes,ydb.granular.create_table,ydb.granular.remove_schema,ydb.granular.alter_schema
ACCESS-ADMINS:ydb.access.grant
DATABASE-ADMINS:ydb.generic.manage
user1:ydb.generic.read
```

Структура результата состоит из трёх блоков:

- `Owner` — показывает владельца объекта схемы.
- `Permissions` — отображает список прав, выданных непосредственно на данный объект.
- `Effective permissions` — отображает список прав, фактически действующих на данный схемный объект с учётом правил наследования прав.  Данный список также включает все права, отображаемые в секции `Permissions`.

## clear

Команда `clear` позволяет отозвать все ранее выданные права на схемный объект. Права, действующие на него по правилам наследования, продолжат действовать.

```bash
{{ ydb-cli }} [global options...] scheme permissions clear [options...] <path>
```

Параметры:
`<path>` — полный путь от корня кластера до объекта, права на который необходимо отозвать.

Например, если над состоянием базы данных из предыдущего примера [`list`](#list) выполнить команду:

```bash
{{ ydb-cli }} scheme permissions clear '/Root/db1/MyApp'
```

И затем заново выполнить команду `list` на объект `/Root/db1/MyApp`, то получим следующий результат:

```bash
Owner: root

Permissions:
none

Effective permissions:
USERS:ydb.database.connect
METADATA-READERS:ydb.generic.list
DATA-READERS:ydb.granular.select_row
DATA-WRITERS:ydb.tables.modify
DDL-ADMINS:ydb.granular.create_directory,ydb.granular.write_attributes,ydb.granular.create_table,ydb.granular.remove_schema,ydb.granular.alter_schema
ACCESS-ADMINS:ydb.access.grant
DATABASE-ADMINS:ydb.generic.manage
```

Обратите внимание, что секция `Permissions` теперь пуста. То есть все права на данный объект были отозваны. Также произошли изменения в содержании секции`Effective permissions`: в ней более не указаны права, которые были выданы непосредственно на объект `/Root/db1/MyApp`.

## chown

Команда `chown` позволяет сменить владельца для схемного объекта.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions chown [options...] <path> <owner>
```

Параметры:
`<path>` — полный путь от корня кластера до объекта, права на который необходимо модифицировать;
`<owner>` — имя нового владельца указанного объекта, может быть пользователем или группой.

Пример команды `chown`:

```bash
{{ ydb-cli }} scheme permissions chown '/Root/db1' testuser
```

{% note info %}

В текущей версии {{ ydb }} есть ограничение, что только непосредственно пользователь - текущий владелец схемного объекта может сменить владельца.

{% endnote %}

## clear-inheritance

Команда `clear-inheritance` позволяет запретить наследование разрешений для схемного объекта.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions clear-inheritance [options...] <path>
```

Параметры:
`<path>` — полный путь от корня кластера до объекта, права на который необходимо модифицировать.

Пример команды `clear-inheritance`:

```bash
{{ ydb-cli }} scheme permissions clear-inheritance '/Root/db1'
```

## set-inheritance

Команда `set-inheritance` позволяет включить наследование разрешений для схемного объекта.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions set-inheritance [options...] <path>
```

Параметры:
`<path>` — полный путь от корня кластера до объекта, права на который необходимо модифицировать.

Пример команды `set-inheritance`:

```bash
{{ ydb-cli }} scheme permissions set-inheritance '/Root/db1'
```

# Разрешения

## Общий перечень команд

Получить список доступных команд можно через интерактивную справку:
```
{{ ydb-cli }} scheme permissions  --help
```

```
Usage: ydb [global options...] scheme permissions [options...] <subcommand>

Description: Modify permissions

Subcommands:
permissions                 Modify permissions
├─ chown                    Change owner
├─ clear                    Clear permissions
├─ grant                    Grant permission (aliases: add)
├─ list                     List permissions
├─ revoke                   Revoke permission (aliases: remove)
└─ set                      Set permissions
```

## grant, revoke

Команды `grant` и `revoke` позволяют установить и отозвать, соответственно, права доступа к объектам схемы для пользователя или группы пользователей.
По сути это аналоги соответствующих YQL-команд [GRANT](../../../yql/reference/syntax/grant.md) и [REVOKE](../../../yql/reference/syntax/revoke.md).

Синтаксис команд YDB CLI выглядит следующим образом:

```bash
{{ ydb-cli }} [connection options] scheme permissions grant  [options...] <path> <subject>
{{ ydb-cli }} [connection options] scheme permissions revoke [options...] <path> <subject>
```

Параметры:
`<path>` - полный путь от корня кластера до объекта, права на который необходимо модифицировать
`<subject>` - имя пользователя или группы, для которых изменяются права доступа

Дополнительные параметры `[options...]`:
`--timeout ms` - технологический параметр, задающий таймаут на отклик сервера. Для наших операций не критичен и здесь мы его не применяем.
`{-p|--permission} NAME` – список прав, которые необходимо предоставить(grant) или отозвать(revoke) у пользователя.

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

Значения всех параметров полностью идентично командам [`grant`, `revoke`](#grant-revoke). Но ключевое отличие команды `set` от `grant` и `revoke` заключается в установке на указанный объект ровно тех прав доступа, что перечислены в параметрах `-p (--permission)`. Другие права для указанного пользователя или группы будут отозваны.

Например, ранее пользователю testuser на объект `'/Root/db1'` были выданы права `"ydb.granular.select_row"`, `"ydb.granular.update_row"`, `"ydb.granular.erase_row"`, `"ydb.granular.read_attributes"`, `"ydb.granular.write_attributes"`, `"ydb.granular.create_directory"`. 
Тогда в результате выполнения команды все права на указанный объект будут отозваны (как будто для каждого из прав вызывали `revoke`) и останется только право `"ydb.granular.select_row"`- непосредственно указанное в команде `set`:

```bash
{{ ydb-cli }} scheme permissions set -p "ydb.granular.select_row" '/Root/db1' testuser
```

## list

Команда `list` позволяет вывести права доступа к объектам схемы.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions list [options...] <path>
```

Параметры:
`<path>` - полный путь от корня кластера до объекта, права на который необходимо модифицировать

Дополнительные параметры `[options...]`:
`--timeout ms` - технологический параметр, задающий таймаут на отклик сервера. Для наших операций не критичен и здесь мы его не применяем.

Пример результата выполнения list:

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

Структура результата состоит из трех блоков:

* `Owner` - показывает владельца объекта схемы.
* `Permissions` – отображает список прав, выданных непосредственно на схемный объект.
* `Effective permissions` – отображает список прав, фактически действующих на данный схемный объект с учетом правил наследования прав. 
Данный список также включает все права, отображаемые в секции Permissions.

## clear

Команда `clear` позволяет отозвать все ранее выданные права на схемный объект.

```bash
{{ ydb-cli }} [global options...] scheme permissions clear [options...] <path>
```

Параметры:
`<path>` - полный путь от корня кластера до объекта, права на который необходимо модифицировать

Дополнительные параметры `[options...]`:
`--timeout ms` - технологический параметр, задающий таймаут на отклик сервера. Для наших операций не критичен и здесь мы его не применяем.

Например, если выполнить команду:

```bash
{{ ydb-cli }} scheme permissions clear '/Root/db1/MyApp' 
```

над состоянием БД из прошлого примера [`list`](#list) и заново выполнить команду `list` на объект `/Root/db1/MyApp`, то получим вот такой результат:

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

Обратите внимание, что секция `Permissions` теперь пуста. То есть все права на данный объект были отозваны. Также произошли изменения в содержании `Effective permissions`. Из секции также пропали права, выданные непосредственно на объект `/Root/db1/MyApp`.

## chown

Команда `chown` позволяет сменить владельца для схемного объекта.

Синтаксис команды:

```bash
{{ ydb-cli }} [connection options] scheme permissions chown [options...] <path> <owner>
```

Параметры:
`<path>` - полный путь от корня кластера до объекта, права на который необходимо модифицировать
`<owner>` - имя пользователя или группы – нового владельца указанного объекта

Дополнительные параметры `[options...]`:
`--timeout ms` - технологический параметр, задающий таймаут на отклик сервера. Для наших операций не критичен и здесь мы его не применяем.

{% note info %}

В текущей версии {{ ydb }} есть ограничение, что только непосредственно текущий владелец схемного объекта может сменить владельца
{% endnote %}

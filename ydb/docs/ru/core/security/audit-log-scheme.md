# Аудитное логирование схемных операций

Записи об операциях со схемой (DDL) позволяют наблюдать за изменением объектов схемы.

{% note info "" %}

DDL ([Data Definition Language](https://en.wikipedia.org/wiki/Data_definition_language)) &mdash; подмножество инструкций SQL, которые создают и изменяют объекты схемы.

{% endnote %}

В логе отражаются операции:
- по созданию, изменению и удалению объектов баз данных, директорий, таблиц, топиков и других типов объектов;
- по изменению прав доступа (ACL) на объектах схемы;
- созданию, изменению и удалению пользователей и групп для режима [аутентификации по логину и паролю](../concepts/auth.md#static-credentials).

## Атрибуты записей

| __Атрибут__ | __Описание__ |
|:----|:----|
| `component` | Логирующий компонент, всегда `schemeshard`.
| `remote_address` | IP-адрес клиента, приславшего запрос.
| `subject` | User SID пользователя, от имени которого выполняется операция.
| `database` | Путь базы данных, в которой выполняется операция.
| `operation`| Тип запроса.
| `start_time` | Время начала операции.
| `end_time` | Время окончания операции.
| `status` | Общий статус операции: `SUCCESS` или `ERROR`.
| `detailed_status` | Детальный статус операции.
| `tx_id`| Уникальный идентификатор транзакции.

### Атрибуты по видам операций

Операции со схемой:
| __Вид операции__ | __Атрибут__ | __Описание__ |
|:----|:----|:----|
| _Все_ ||
|| `paths` | Список путей внутри базы данных, которые изменяются операцией (например, `[/root/db/table-a, /root/db/table-b]`).
| _Передача владения, изменение разрешений_ ||
|| `new_owner` | User SID нового владельца объекта при передаче владения.
|| `acl_add` | Список добавленных разрешений при создании объектов или изменении разрешений, в [краткой записи](./short-access-control-notation.md) (например, `[+R:someuser]`).
|| `acl_remove` | Список удаленных разрешений при изменении разрешений, в [краткой записи](./short-access-control-notation.md) (например, `[-R:somegroup]`).
| _Работа с [пользовательскими атрибутами](../concepts/datamodel/table?#users-attr)_ ||
|| `user_attrs_add` | Список добавленных пользовательских атрибутов при создании объектов или изменении атрибутов (например, `[attr1: A, attr2: B]`).
|| `user_attrs_remove` | Список удаленных пользовательских атрибутов при изменении атрибутов (например, `[attr1, attr2]`).

Операции с [учётными данными](../concepts/auth#static-credentials):
| __Атрибут__ | __Описание__ |
|:----|:----|
| `login_user` | Имя пользователя (при [создании или модификации пользователя](./access-management.md#users)).
| `login_group` | Имя группы (при [создании, модификации групп](./access-management.md#groups)).
| `login_member` | Имя пользователя (при [добавлении, удалении из группы](./access-management.md#groups)).

## Включение

Аудитное логирование должно быть [включено](audit-log.md#enabling-audit-log) на кластере.

Логирование схемных операций не требует дополнительных настроек.

## Что нужно знать

- Логирование происходит в компоненте {{ ydb-short-name }} `Scheme Shard`.

[//]: # (TODO: `start_time` и `end_time` содержат время начала и завершения операции.)
- Запись в лог делается в момент приёма операции на исполнение.

- Запрос на создание объект в схеме может породить несколько операций и привести к многим записям в логе (по количеству операций). У всех таких записей будет одинаковый `tx_id`. Такое случается, когда объект создаётся по пути, промежуточные элементы которого тоже ещё не существуют и должны быть созданы (например, если путь `root/a` не существует, то создание таблицы по пути `root/a/table` будет выполняться двумя последовательными операциями: `create root/a` и затем `create root/a/table`).

- В лог попадают запросы, прошедшие [аутентификацию](../deploy/configuration/config#auth) и проверку прав на базу. При отключенной аутентификации запросы логируются, в поле `subject` будет `{none}`.

## Примеры записей лога

```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "DROP TABLE",
    "tx_id": "845026768199165",
    "paths": "[/root/db/table1  ]",
    "status": "SUCCESS",
    "detailed_status": "StatusAccepted"
}
```
```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "CREATE DIRECTORY",
    "tx_id": "563346945792517",
    "paths": "[/root/db/default-exports]",
    "status": "SUCCESS",
    "detailed_status": "StatusAlreadyExists",
    "reason": "Check failed: path: '/root/db/default-exports', error: path exist, request accepts it (id: [...], type: EPathTypeDir, state: EPathStateNoChanges)"
}
```
```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "{none}",
    "database": "/root/db1",
    "operation": "MODIFY ACL",
    "tx_id": "281474976775658",
    "paths": "[/root/db/some_dir]",
    "acl_add": "[+(ConnDB):subject:-]",
    "status": "SUCCESS",
    "detailed_status": "StatusAccepted"
}
```

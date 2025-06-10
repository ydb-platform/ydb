# Права доступа

[Права доступа](../concepts/glossary.md#access-right) в {{ ydb-short-name }} привязаны не к [субъекту](../concepts/glossary.md#access-subject), а к [объекту доступа](../concepts/glossary.md#access-object).

Для каждого объекта доступа ведется специальный список— [ACL](../concepts/glossary.md#access-acl) (Access Control List) — он хранит все предоставленные [субъектам доступа](../concepts/glossary.md#subject) (пользователям и группам) права на объект.

По умолчанию, права наследуются от родителей потомкам по дереву схемных объектов.

Для управления правами служат следующие виды YQL запросов:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

Для управления правами служат следующие CLI-команды:

* [chown](../reference/ydb-cli/commands/scheme-permissions.md#chown)
* [grant](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [revoke](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [set](../reference/ydb-cli/commands/scheme-permissions.md#set)
* [clear](../reference/ydb-cli/commands/scheme-permissions.md#clear)
* [clear-inheritance](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance)
* [set-inheritance](../reference/ydb-cli/commands/scheme-permissions.md#set-inheritance)

Для просмотра ACL объекта доступа служат следующие CLI-команды:

* [describe](../reference/ydb-cli/commands/scheme-describe.md)
* [list](../reference/ydb-cli/commands/scheme-permissions.md#list)

## Смотрите также

* [{#T}](./short-access-control-notation.md)
# Авторизация

## Основные понятия

Авторизация в {{ ydb-short-name }} основана на понятиях:

* [Объект доступа](../concepts/glossary.md#access-object)
* [Субъект доступа](../concepts/glossary.md#access-subject)
* [Права доступа](../concepts/glossary.md#access-right)
* [Список доступов](../concepts/glossary.md#access-acl)
* [Владелец](../concepts/glossary.md#access-owner)
* [Пользователь](../concepts/glossary.md#access-user)
* [Группа](../concepts/glossary.md#access-group)

Независимо от метода [аутентификации](https://ru.wikipedia.org/wiki/Аутентификация), [авторизация](https://ru.wikipedia.org/wiki/Авторизация) всегда выполняется на серверной стороне {{ ydb-short-name }} на основе хранящейся в ней информации об объектах и правах доступа. Права доступа определяют набор доступных для выполнения операций.

Авторизация выполняется на каждое действие пользователя: его права не кешируются, так как могут быть отозваны или предоставлены в любой момент времени.

## Пользователь {#user}

Пользователи в {{ ydb-short-name }} могут создаваться в разных источниках:

- локальные пользователи в базах данных {{ ydb-short-name }};
- внешние пользователи из сторонних служб доступа к каталогам.

Для создания, изменения и удаления [локальных пользователей](../concepts/glossary.md#access-user) {{ ydb-short-name }} есть команды:

* [{#T}](../yql/reference/syntax/create-user.md)
* [{#T}](../yql/reference/syntax/alter-user.md)
* [{#T}](../yql/reference/syntax/drop-user.md)

{% include [!](../_includes/do-not-create-users-in-ldap.md) %}

{% note info %}

Отдельно выделяется пользователь `root` с максимальными правами. Он создаётся при первоначальном развёртывании кластера, в ходе которой ему нужно сразу установить пароль. В дальнейшем использование данной учетной записи не рекомендуется и следует завести пользователей с ограниченными правами.

Подробнее про первоначальное развертывание:

* [Ansible](../devops/deployment-options/ansible/initial-deployment/index.md)
* [Kubernetes](../devops/deployment-options/kubernetes/initial-deployment.md)
* [Вручную](../devops/deployment-options/manual/initial-deployment/index.md)
* [{#T}](./builtin-security.md)

{% endnote %}

### SID {#sid}

{{ ydb-short-name }} позволяет работать с [пользователями](../concepts/glossary.md#access-user) из разных каталогов и систем, и они отличаются [SID](../concepts/glossary.md#access-sid) с использованием суффикса.

Суффикс `@<auth-domain>` идентифицирует «источник пользователя», внутри которого гарантируется уникальность всех логинов или идентификаторов пользователей. Например, в случае [аутентификации LDAP](authentication.md#ldap-auth-provider) SID'ы пользователей будут `user1@ldap` и `user2@ldap`.<br/>
У локальных пользователей пустой auth-domain. Если SID пользователя не содержит суффикса, то имеется в виду локальный пользователь, созданный и существующий непосредственно в кластере {{ ydb-short-name }}.

## Группа {#group}

Любого [пользователя](../concepts/glossary.md#access-user) можно включить в ту или иную [группу доступа](../concepts/glossary.md#access-group) или исключить из неё. Как только пользователь включается в группу, он получает все права на [объекты базы данных](../concepts/glossary.md#access-object), которые предоставлялись группе доступа.
С помощью групп доступа {{ ydb-short-name }} можно реализовать бизнес-роли пользовательских приложений, заранее настроив требуемые права доступа на нужные объекты.

{% note info %}

Группа доступа может быть пустой, когда в неё не входит ни один пользователь.

Группы доступа могут быть вложенными.

{% endnote %}

Для создания, изменения и удаления [групп](../concepts/glossary.md#access-group) есть следующие виды YQL запросов:

* [{#T}](../yql/reference/syntax/create-group.md)
* [{#T}](../yql/reference/syntax/alter-group.md)
* [{#T}](../yql/reference/syntax/drop-group.md)

## Права доступа {#right}

[Права доступа](../concepts/glossary.md#access-right) в {{ ydb-short-name }} привязаны не к [субъекту](../concepts/glossary.md#access-subject), а к [объекту доступа](../concepts/glossary.md#access-object).

У каждого объекта доступа есть список прав — [ACL](../concepts/glossary.md#access-control-list) (Access Control List) — он хранит все предоставленные [субъектам доступа](../concepts/glossary.md#subject) (пользователям и группам) права на объект.

По умолчанию, права наследуются от родителей потомкам по дереву объектов доступа.

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

## Владелец объекта {#owner}

У каждого объекта доступа есть [владелец](../concepts/glossary.md#access-owner). Им по умолчанию становится [субъект доступа](../concepts/glossary.md#access-subject), создавший [объект доступа](../concepts/glossary.md#access-object).

{% note info %}

Для владельца не проверяются [списки прав](../concepts/glossary.md#access-control-list) на данный [объект доступа](../concepts/glossary.md#access-object).

Он имеет полный набор прав на объект.

{% endnote %}

Владелец объекта есть в том числе у кластера в целом и каждой базы данных.

Сменить владельца можно с помощью CLI команды [`chown`](../reference/ydb-cli/commands/scheme-permissions.md#chown).

Просматривать владельца объекта можно с помощью CLI команды [`describe`](../reference/ydb-cli/commands/scheme-describe.md).

## Списки уровней доступа {#access-level-lists}

В дополнение к [спискам прав](../concepts/glossary.md#access-control-list), управляющим доступом к конкретным [схемным объектам](../concepts/glossary.md#scheme-object), {{ ydb-short-name }} использует [списки уровней доступа](../concepts/glossary.md#access-level-list) для определения иерархических уровней доступа к общекластерным операциям.

Для операций, в которых одновременно проверяются [списки прав](../concepts/glossary.md#access-control-list) и [списки уровней доступа](../concepts/glossary.md#access-level-list), оба механизма применяются совместно: действие доступно только если обе проверки его разрешают, и недоступно, если хотя бы одна проверка не пройдена. Для остальных операций применяется только соответствующий механизм проверки.

### Иерархия уровней доступа

Списки уровней доступа образуют иерархию, которая используется в [Embedded UI](../reference/embedded-ui/ydb-monitoring.md), viewer и во многих других общекластерных действиях (порядок от меньших привилегий к большим):

- `database_allowed_sids` (`Database`) - доступ к операциям в контексте конкретной базы;
- `viewer_allowed_sids` (`Viewer`) - доступ к просмотру общекластерного состояния;
- `monitoring_allowed_sids` (`Monitoring`) - доступ к операционным действиям в Embedded UI;
- `administration_allowed_sids` (`Administration`) - административные действия с кластером и базами.

Более высокий уровень автоматически включает все более низкие, поэтому субъекту достаточно присутствовать только в одном списке. Например, наличие в `administration_allowed_sids` автоматически даёт привилегии `monitoring`, `viewer` и `database`.
Подробности по каждому уровню — в разделе [Описание уровней доступа](#access-level-descriptions).

Дополнительно существуют два отдельных списка уровней доступа для специфических операций:

- `bootstrap_allowed_sids` — разрешает операции начальной инициализации кластера;
- `register_dynamic_node_allowed_sids` — разрешает регистрацию узлов в кластере.

### Описание уровней доступа {#access-level-descriptions}

Списки уровней доступа настраиваются в [конфигурации безопасности](../reference/configuration/security_config.md#security-access-levels) и определяют привилегии для:

- **Database** (наличие в `database_allowed_sids`) — доступ только в контексте конкретной базы данных. Можно открыть Embedded UI и работать с данными этой базы, но нельзя выполнять общекластерные запросы (например, просматривать список узлов кластера). Запросы без указания базы запрещены.
- **Viewer** (наличие в `viewer_allowed_sids`) — доступ только на чтение для общекластерного состояния: можно просматривать страницы [Embedded UI](../reference/embedded-ui/ydb-monitoring.md) и диагностическую информацию, но нельзя запускать действия, изменяющие состояние системы.
- **Monitoring** (наличие в `monitoring_allowed_sids`) — доступ к операционным действиям в Embedded UI, включая действия, которые могут менять состояние системы. Например, запуск резервного копирования, восстановление базы или выполнение YQL-запросов через Embedded UI.
- **Administration** (наличие в `administration_allowed_sids`) — даёт право выполнять административные действия с базами данных или кластером. Полный административный доступ к кластеру и его базам данных. Также используется для изменения конфигурации, схемных операций, требующих административных прав, и других административных проверок.
- **Register node** (наличие в `register_dynamic_node_allowed_sids`) — отдельный (неиерархический) уровень для регистрации динамических узлов в кластере. Не даёт автоматически прав `database`/`viewer`/`monitoring`/`administration`. По техническим причинам, если список задан (не пуст), он должен включать `root@builtin`.
- **Bootstrap** (наличие в `bootstrap_allowed_sids`) — отдельный (неиерархический) уровень только для операций начальной инициализации кластера. Используется в неинициализированном состоянии, когда подсистема аутентификации ещё не функционирует. Начальная инициализация разрешена, если субъект входит в `bootstrap_allowed_sids` или `administration_allowed_sids`, при этом сам по себе `bootstrap` не выдаёт полные административные привилегии.


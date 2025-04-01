# {{ ydb-short-name }} for Security Engineers

This section of {{ ydb-short-name }} documentation covers security-related aspects of working with {{ ydb-short-name }}. It'll be useful for compliance purposes too.

![Eagle-view diagram](./_assets/security-overview.png)

- **[Authentication](./authentication.md) and [authorization](./authorization.md)**. The access control system in {{ ydb-short-name }} provides data protection in a {{ ydb-short-name }} cluster. Due to the access system, only authorized [access subjects](../concepts/glossary.md#access-subject) (users and groups) can work with data. Access to data can be restricted.

    When a [user](../concepts/glossary.md#access-user) connects to a {{ ydb-short-name }} database, {{ ydb-short-name }} first identifies the user's account. This process is called [authentication](./authentication.md). Based on the authentication data, a user then goes through [authorization](./authorization.md) — a process that verifies whether a user has sufficient [access rights](../concepts/glossary.md#access-right) and [access levels](../concepts/glossary.md#access-level) to perform user operations.

    {{ ydb-short-name }} supports both internal [users](./authorization.md#user) and external users from third-party directory services. After passing [authentication](./authentication.md), a user gets a [SID](./authorization.md#sid) that the {{ ydb-short-name }} cluster uses for user identification and access control.

    [Access rights](./authorization.md#right) in {{ ydb-short-name }} are tied to [access objects](../concepts/glossary.md#access-object) using [access control lists (ACL)](../concepts/glossary.md#access-control-list). The ACL format is described in [{#T}](./short-access-control-notation.md).

    {{ ydb-short-name }} uses [access levels](../concepts/glossary.md#access-level) to manage [access subject](../concepts/glossary.md#access-subject) privileges that are not related to [scheme objects](../concepts/glossary.md#scheme-object). Access levels are granted to users in [access level lists](../concepts/glossary.md#access-level-list)

    [Built-in security](./builtin-security.md) is configured automatically by default when the {{ ydb-short-name }} cluster is started for the first time. This process adds a [superuser](./builtin-security.md#superuser) and a set of [roles](./builtin-security.md#role) for convenient user access management.

- **Encryption**. As {{ ydb-short-name }} is a distributed system typically running on a cluster, often spanning multiple datacenters or availability zones, user data is routinely transferred over the network. Various protocols can be involved, and each can be configured to use [encryption in transit](./encryption/data-in-transit.md). {{ ydb-short-name }} also supports transparent [data encryption at rest](./encryption/data-at-rest.md) at the [DS proxy](../concepts/glossary.md#ds-proxy) level.

- **Audit logs**. {{ ydb-short-name }} provides [audit logs](./audit-log.md) that include data about all operations that attempted to change the {{ ydb-short-name }} objects, whether successful or not.

Main resources:

- [{#T}](authentication.md)
- [{#T}](authorization.md)
- [{#T}](builtin-security.md)
- [{#T}](audit-log.md)
- Encryption:

  - [{#T}](encryption/data-at-rest.md)
  - [{#T}](encryption/data-in-transit.md)

- [{#T}](short-access-control-notation.md)
- Concepts:

  - [{#T}](../concepts/connect.md)

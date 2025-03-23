# {{ ydb-short-name }} for Security Engineers

This section of {{ ydb-short-name }} documentation covers security-related aspects of working with {{ ydb-short-name }}. It'll be useful for compliance purposes too.

![Eagle-view diagram](./_assets/rbac.png)

- **[Authentication](./authentication.md) and [authorization](./authorization.md)**. The access control system in {{ydb-short-name }} provides data protection in a {{ydb-short-name }} cluster. Due to the access system, only authorized [access subjects](../concepts/glossary.md#access-subject) (users and groups) can work with data. Access to data can be restricted.

    When a [user](../concepts/glossary.md#access-user) connects to a {{ ydb-short-name }} database, {{ ydb-short-name }} first identifies the user's account. This process is called [authentication](./authentication.md). Based on the authentication data a user then goes through [authorization](./authorization.md) — {{ ydb-short-name }} grants certain [access rights](../concepts/glossary.md#access-right) and [access levels](../concepts/glossary.md#access-level) to a user for {{ ydb-short-name }} database objects.

    {{ ydb-short-name }} supports both internal [users](./authorization.md#user) and external users from third-party directory services. Having passed [authentication](./authentication.md), a {{ ydb-short-name }} cluster identifies users by [SIDs](./authorization.md#sid). A SID is a string that contains a user name and auth domain.

    [Access rights](./authorization.md#right) in {{ ydb-short-name }} are tied to [access objects](../concepts/glossary.md#access-object) using [access control lists (ACL)](../concepts/glossary.md#access-control-list). The ACL format is described in [{#T}](./short-access-control-notation.md).

    {{ ydb-short-name }} uses [access levels](../concepts/glossary.md#access-level) for managing [access subject](#access-subject) privileges that are not related to [scheme objects](#scheme-object).

    [Built-in security](./builtin-security.md) is configured automatically when the {{ ydb-short-name }} cluster is started for the first time by default. This process adds a [superuser](./builtin-security.md#superuser) and a set of [roles](./builtin-security.md#role) for convenient user access management.

- **Encryption**. As {{ ydb-short-name }} is a distributed system typically running on a cluster, often spanning multiple datacenters or availability zones, user data is routinely transferred over the network. Various protocols can be involved, and each can be configured to use [encryption in transit](./encryption/data-in-transit.md). {{ ydb-short-name }} also supports transparent [data encryption at rest](./encryption/data-at-rest.md) at the [DS proxy](../concepts/glossary.md#ds-proxy) level.

- **Audit logs**. {{ ydb-short-name }} provides [audit logs](./audit-log.md) that include data about all the operations that tried to change the {{ ydb-short-name }} objects, successfully or unsuccessfully.

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

# Configuring built-in security

Built-in security is configured automatically when the {{ ydb-short-name }} cluster is started for the first time, if the [`security_config`](../reference/configuration/index.md#security) section in the cluster configuration file does not define the `default_users`, `default_groups`, `default_access` parameters.

To bypass configuring built-in security before the {{ ydb-short-name }} cluster starts for the first time, set the [`domains_config.disable_builtin_security`](../reference/configuration/index.md#domains-config) parameter to `true`.

The built-in security adds a superuser and configures a set of roles for user access management.

## Roles

Role | Description
--- | ---
`ADMINS` | Provides unlimited access rights for the entire {{ ydb-short-name }} cluster scheme.
`DATABASE-ADMINS` | Provides access rights to manage databases, scheme, and scheme access rights. No data access.
`ACCESS-ADMINS` | Provides access rights to manage scheme access rights. No data access.
`DDL-ADMINS` | Provides access rights to manage the scheme. No data access.
`DATA-WRITERS` | Provides access rights for scheme objects, reading and changing data.
`DATA-READERS` | Provides access rights for scheme objects and reading data.
`METADATA-READERS` | Provides access rights for scheme objects. No data access.
`USERS` | Provides access rights for databases. It's a common group for all users.

## Groups

Roles in {{ ydb-short-name }} are implemented as a hierarchy of [user](../concepts/glossary.md#access-user) [groups](./authorization.md#group) and a set of [access rights](./authorization.md#right) for these groups. Access rights for the groups are granted on the cluster scheme root.

Groups can be nested. A child group inherits access rights of the parent group:

{% include notitle [builtin-groups-graph](../_includes/builtin-groups-graph.md) %}

For example, users of the `DATA-WRITERS` group are allowed:

- to view the scheme — `METADATA-READERS`
- to read data — `DATA-READERS`
- to change data — `DATA-WRITERS`

Users of the `DDL-ADMINS` group are allowed:

- to view the scheme — `METADATA-READERS`
- to change the scheme — `DDL-ADMINS`

Users of the `ADMINS` group are allowed to perform all operations with the scheme and data.

## Superuser

A superuser belongs to the `ADMINS` and `USERS` groups and has full access rights for the cluster scheme.

By default, a superuser is the `root` user with an empty password.

## A group for all users

The `USERS` group is also defined as a common [group](../concepts/glossary.md#access-group) of all local [users](../concepts/glossary.md#access-user). When you [add new users](./authorization.md#user) later, they will be automatically added to the `USERS` group.


For more information about managing groups and users, see [{#T}](../security/authorization.md).
# Initial cluster security configuration

Initial security is configured automatically when the {{ ydb-short-name }} cluster starts for the first time.

During this process {{ ydb-short-name }} adds a [superuser](#superuser) and a set of [roles](#roles) for user access management.

{% note info %}

For information about overriding and skipping initial security configuration, see the following sections:

- [{#T}](#skip-initial-security)
- [{#T}](#override-initial-security)

{% endnote %}

## Roles {#roles}

| Role              | Description |
|------------------|-------------|
| `ADMINS`        | Provides unlimited access rights for the entire {{ ydb-short-name }} cluster scheme. |
| `DATABASE-ADMINS` | Provides access rights to manage databases, their scheme, and scheme access rights. No data access. |
| `ACCESS-ADMINS`  | Provides access rights to manage scheme access rights. No data access. |
| `DDL-ADMINS`    | Provides access rights to manage the scheme. No data access. |
| `DATA-WRITERS`  | Provides access rights for scheme objects, including reading and modifying data. |
| `DATA-READERS`  | Provides access rights for scheme objects and reading data. |
| `METADATA-READERS` | Provides access rights for scheme objects. No data access. |
| `USERS`         | Provides access rights for databases. This is a common group for all users. |

## Groups {#groups}

Roles in {{ ydb-short-name }} are implemented as a hierarchy of [user](../concepts/glossary.md#access-user) [groups](./authorization.md#group) and a set of [access rights](./authorization.md#right) for these groups. Access rights for the groups are granted on the cluster scheme root.

Groups can be nested, and a child group inherits the access rights of its parent group:

{% include notitle [builtin-groups-graph](../_includes/builtin-groups-graph.md) %}

For example, users in the `DATA-WRITERS` group are allowed to:

- View the scheme — `METADATA-READERS`
- Read data — `DATA-READERS`
- Change data — `DATA-WRITERS`

Users in the `DDL-ADMINS` group are allowed to:

- View the scheme — `METADATA-READERS`
- Change the scheme — `DDL-ADMINS`

Users in the `ADMINS` group are allowed to perform all operations on the scheme and data.

## Superuser {#superuser}

A superuser belongs to the `ADMINS` and `USERS` groups and has full access rights to the cluster scheme.

By default, a superuser is the `root` user with an empty password.

## A group for all users {#all-users-group}

The `USERS` group is a common [group](../concepts/glossary.md#access-group) for all local [users](../concepts/glossary.md#access-user). When you [add new users](./authorization.md#user), they are automatically added to the `USERS` group.

For more information about managing groups and users, see [{#T}](../security/authorization.md).

## Overriding initial security configuration {#override-initial-security}

You can override the initial security configuration with a custom set of users, groups, and access rights.

To specify custom users, groups, and access rights to be created during the initial security configuration, define the `default_users`, `default_groups`, or `default_access` parameters in the [`security_config`](../reference/configuration/security_config.md#security-bootstrap) section in the cluster configuration file.

## Skipping initial security configuration {#skip-initial-security}

You can skip initial security configuration by setting the [`security_config.disable_builtin_security`](../reference/configuration/domains_config.md#domains-config) parameter to `true`.
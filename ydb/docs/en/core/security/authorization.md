# Authorization

## Basic concepts

Authorization in {{ ydb-short-name }} is based on the following concepts:

* [Access object](../concepts/glossary.md#access-object)
* [Access subject](../concepts/glossary.md#access-subject)
* [Access rights](../concepts/glossary.md#access-right)
* [Access list](../concepts/glossary.md#access-acl)
* [Owner](../concepts/glossary.md#access-owner)
* [User](../concepts/glossary.md#access-user)
* [Group](../concepts/glossary.md#access-group)

Regardless of the [authentication](https://en.wikipedia.org/wiki/Authentication) method, [authorization](https://en.wikipedia.org/wiki/Authorization) is always performed on the server side of {{ ydb-short-name }} based on the information about objects and access rights stored in it. Access rights determine the set of operations that can be performed.

Authorization is performed for every user action: their rights are not cached, as they can be revoked or granted at any time.

## User {#user}

Users in {{ ydb-short-name }} can be created in different sources:

- local users in {{ ydb-short-name }} databases
- external users from third-party directory access services.

To create, modify, and delete [local users](../concepts/glossary.md#access-user), {{ ydb-short-name }} provides the following commands:

* [{#T}](../yql/reference/syntax/create-user.md)
* [{#T}](../yql/reference/syntax/alter-user.md)
* [{#T}](../yql/reference/syntax/drop-user.md)

{% include [!](../_includes/do-not-create-users-in-ldap.md) %}

{% note info %}

The `root` user with maximum privileges is set apart. This user is created during the initial cluster deployment, during which a password must be set immediately. Further use of this account is not recommended; instead, you should create users with limited privileges.

For more details on initial deployment:

* [Ansible](../devops/deployment-options/ansible/initial-deployment/index.md)
* [Kubernetes](../devops/deployment-options/kubernetes/initial-deployment.md)
* [Manually](../devops/deployment-options/manual/initial-deployment/index.md)
* [{#T}](./builtin-security.md)

{% endnote %}

### SID {#sid}

{{ ydb-short-name }} allows working with [users](../concepts/glossary.md#access-user) from different directories and systems, and they are distinguished by [SID](../concepts/glossary.md#access-sid) using a suffix.

The `@<auth-domain>` suffix identifies the 'user source', within which the uniqueness of all logins or user identifiers is guaranteed. For example, in the case of [LDAP authentication](authentication.md#ldap-auth-provider), user SIDs will be `user1@ldap` and `user2@ldap`.<br/>
Local users have an empty auth-domain. If a user SID does not contain a suffix, it refers to a local user created and existing directly in the {{ ydb-short-name }} cluster.

## Group {#group}

Any [user](../concepts/glossary.md#access-user) can be added to a particular [access group](../concepts/glossary.md#access-group) or removed from it. As soon as a user is added to a group, they receive all rights to [database objects](../concepts/glossary.md#access-object) that were granted to the access group.
Using access groups, {{ ydb-short-name }} can implement business roles of user applications by pre-configuring the required access rights to the necessary objects.

{% note info %}

An access group can be empty, meaning no users are included in it.

Access groups can be nested.

{% endnote %}

The following types of YQL queries are available for creating, modifying, and deleting [groups](../concepts/glossary.md#access-group):

* [{#T}](../yql/reference/syntax/create-group.md)
* [{#T}](../yql/reference/syntax/alter-group.md)
* [{#T}](../yql/reference/syntax/drop-group.md)

## Access rights {#right}

In {{ ydb-short-name }}, [access rights](../concepts/glossary.md#access-right) are tied not to the [subject](../concepts/glossary.md#access-subject), but to the [access object](../concepts/glossary.md#access-object).

Each access object has a list of rights — [ACL](../concepts/glossary.md#access-acl) (Access Control List) — it stores all rights granted to [access subjects](../concepts/glossary.md#subject) (users and groups) on the object.

By default, rights are inherited from parents to children along the access object tree.

The following types of YQL queries are used to manage rights:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

The following CLI commands are used to manage rights:

* [chown](../reference/ydb-cli/commands/scheme-permissions.md#chown)
* [grant](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [revoke](../reference/ydb-cli/commands/scheme-permissions.md#grant-revoke)
* [set](../reference/ydb-cli/commands/scheme-permissions.md#set)
* [clear](../reference/ydb-cli/commands/scheme-permissions.md#clear)
* [clear-inheritance](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance)
* [set-inheritance](../reference/ydb-cli/commands/scheme-permissions.md#set-inheritance)

The following CLI commands are used to view the ACL of an access object:

* [describe](../reference/ydb-cli/commands/scheme-describe.md)
* [list](../reference/ydb-cli/commands/scheme-permissions.md#list)

## Object owner {#owner}

Each access object has an [owner](../concepts/glossary.md#access-owner). By default, it is the [access subject](../concepts/glossary.md#access-subject) that created the [access object](../concepts/glossary.md#access-object).

{% note info %}

For the owner, [access control lists](../concepts/glossary.md#access-control-list) are not checked for the given [access object](../concepts/glossary.md#access-object).

The owner has the full set of rights on the object.

{% endnote %}

The object owner also exists for the cluster as a whole and for each database.

You can change the owner using the CLI command [`chown`](../reference/ydb-cli/commands/scheme-permissions.md#chown).

You can view the object owner using the CLI command [`describe`](../reference/ydb-cli/commands/scheme-describe.md).

## Access level lists {#access-level-lists}

In addition to [access control lists](../concepts/glossary.md#access-control-list) that control access to specific [schema objects](../concepts/glossary.md#scheme-object), {{ ydb-short-name }} uses [access level lists](../concepts/glossary.md#access-level-list) to define hierarchical access levels to cluster-wide operations.

For operations that check both [access control lists](../concepts/glossary.md#access-control-list) and [access level lists](../concepts/glossary.md#access-level-list), both mechanisms are applied together: an action is available only if both checks allow it, and is unavailable if at least one check fails. For other operations, only the corresponding check mechanism is applied.

### Access level hierarchy

Access level lists form a hierarchy that is used in [{{ ydb-ui-name }}](../reference/ydb-ui/ydb-monitoring.md), viewer, and many other cluster-wide actions (ordered from least to most privileges):

- `database_allowed_sids` (`Database`): access to operations in the context of a specific database.
- `viewer_allowed_sids` (`Viewer`): access to viewing the cluster-wide state.
- `monitoring_allowed_sids` (`Monitoring`): access to operational actions in {{ ydb-ui-name }}.
- `administration_allowed_sids` (`Administration`): administrative actions on the cluster and databases.

A higher level automatically includes all lower ones, so a subject only needs to be present in one list. For example, being in `administration_allowed_sids` automatically grants the privileges of `monitoring`, `viewer`, and `database`.
Details on each level are in the section [Description of access levels](#access-level-descriptions).

Additionally, there are two separate access level lists for specific operations:

- `bootstrap_allowed_sids` — allows cluster initialization operations.
- `register_dynamic_node_allowed_sids` — allows node registration in the cluster.

### Description of access levels {#access-level-descriptions}

Access level lists are configured in [security configuration](../reference/configuration/security_config.md#security-access-levels) and define privileges for:

- **Database** (presence in `database_allowed_sids`) — access only in the context of a specific database. You can open {{ ydb-ui-name }} and work with the data of this database, but you cannot run cluster-wide queries (for example, view the list of cluster nodes). Queries without specifying a database are prohibited.
- **Viewer** (presence in `viewer_allowed_sids`) — read-only access to the cluster-wide state: you can view the pages [{{ ydb-ui-name }}](../reference/ydb-ui/ydb-monitoring.md) and diagnostic information, but you cannot run actions that change the system state.
- **Monitoring** (presence in `monitoring_allowed_sids`) — access to operational actions in {{ ydb-ui-name }}, including actions that can change the system state. For example, starting a backup, restoring a database, or running YQL queries via {{ ydb-ui-name }}.
- **Administration** (presence in `administration_allowed_sids`) — grants the right to perform administrative actions on databases or the cluster. Full administrative access to the cluster and its databases. Also used for changing configuration, schema operations that require administrative rights, and other administrative checks.
- **Register node** (presence in `register_dynamic_node_allowed_sids`) — a separate (non-hierarchical) level for registering dynamic nodes in the cluster. It does not automatically grant the rights `database`/`viewer`/`monitoring`/`administration`. For technical reasons, if the list is specified (not empty), it must include `root@builtin`.
- **Bootstrap** (presence in `bootstrap_allowed_sids`) — a separate (non-hierarchical) level only for cluster initialization operations. Used in an uninitialized state when the authentication subsystem is not yet functioning. Initialization is allowed if the subject is in `bootstrap_allowed_sids` or `administration_allowed_sids`, while `bootstrap` itself does not grant full administrative privileges.

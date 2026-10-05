# Authorization

## Basic concepts

Authorization in {{ ydb-short-name }} is based on the concepts of:

* [Access object](../concepts/glossary.md#access-object)
* [Access subject](../concepts/glossary.md#access-subject)
* [Access right](../concepts/glossary.md#access-right)
* [Access control list](../concepts/glossary.md#access-acl)
* [Owner](../concepts/glossary.md#access-owner)
* [User](../concepts/glossary.md#access-user)
* [Group](../concepts/glossary.md#access-group)

Regardless of the [authentication](https://en.wikipedia.org/wiki/Authentication) method, [authorization](https://en.wikipedia.org/wiki/Authorization) is always performed on the server side of {{ ydb-short-name }} based on the stored information about access objects and rights. Access rights determine the set of operations available to perform.

Authorization is performed for each user action: the rights are not cached, as they can be revoked or granted at any time.

## User {#user}

To create, alter, and delete users in {{ ydb-short-name }}, the following commands are available:

* [{#T}](../yql/reference/syntax/create-user.md)
* [{#T}](../yql/reference/syntax/alter-user.md)
* [{#T}](../yql/reference/syntax/drop-user.md)

{% include [!](../_includes/do-not-create-users-in-ldap.md) %}

{% note info %}

There is a separate user `root` with maximum rights. It is created during the initial deployment of the cluster, during which a password must be set immediately. It is not recommended to use this account long-term; instead, users with limited rights should be created.

More about initial deployment:

* [Ansible](../devops/deployment-options/ansible/initial-deployment/index.md)
* [Kubernetes](../devops/deployment-options/kubernetes/initial-deployment.md)
* [Manually](../devops/deployment-options/manual/initial-deployment/index.md)

{% endnote %}

{{ ydb-short-name }} allows working with [users](../concepts/glossary.md#access-user) from different directories and systems, and they differ by [SID](../concepts/glossary.md#access-sid) using a suffix.

The suffix `@<subsystem>` identifies the "user source" or "auth domain", within which the uniqueness of all `login` is guaranteed. For example, in the case of [LDAP authentication](authentication.md#ldap-auth-provider), user names will be `user1@ldap` and `user2@ldap`.
If a `login` without a suffix is specified, it implies users directly created in the {{ ydb-short-name }} cluster.

## Group {#group}

Any [user](../concepts/glossary.md#access-user) can be included in or excluded from a certain [access group](../concepts/glossary.md#access-group). Once a user is included in a group, they receive all the rights to [database objects](../concepts/glossary.md#access-object) that were provided to the access group.
With access groups in {{ ydb-short-name }}, business roles for user applications can be implemented by pre-configuring the required access rights to the necessary objects.

{% note info %}

An access group can be empty when it does not include any users.

Access groups can be nested.

{% endnote %}

To create, alter, and delete [groups](../concepts/glossary.md#access-group), the following types of YQL queries are available:

* [{#T}](../yql/reference/syntax/create-group.md)
* [{#T}](../yql/reference/syntax/alter-group.md)
* [{#T}](../yql/reference/syntax/drop-group.md)

## Right {#right}

[Rights](../concepts/glossary.md#access-right) in {{ ydb-short-name }} are tied not to the [subject](../concepts/glossary.md#access-subject), but to the [access object](../concepts/glossary.md#access-object).

Each access object has a list of permissions — [ACL](../concepts/glossary.md#access-acl) (Access Control List) — it stores all the rights provided to [access subjects](../concepts/glossary.md#subject) (users and groups) for the object.

By default, rights are inherited from parents to descendants in the access objects tree.

The following types of YQL queries are used for managing rights:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

The following CLI commands are used for managing rights:

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

## Object Owner {#owner}

Each access object has an [owner](../concepts/glossary.md#access-owner). By default, it becomes the [access subject](../concepts/glossary.md#access-subject) who created the [access object](../concepts/glossary.md#access-object).

{% note info %}

For the owner, [permission lists](../concepts/glossary.md#access-control-list) on this [access object](../concepts/glossary.md#access-object) are not checked.

They have a full set of rights on the object.

{% endnote %}

An object owner exists for the entire cluster and each database.

The owner can be changed using the CLI command [`chown`](../reference/ydb-cli/commands/scheme-permissions.md#chown).

<<<<<<< HEAD
The owner of an object can be viewed using the CLI command [`describe`](../reference/ydb-cli/commands/scheme-describe.md).
=======
You can view the object owner using the CLI command [`describe`](../reference/ydb-cli/commands/scheme-describe.md).

## Access level lists {#access-level-lists}

In addition to [access control lists](../concepts/glossary.md#access-control-list) that manage access to specific [schema objects](../concepts/glossary.md#scheme-object), {{ ydb-short-name }} uses [access level lists](../concepts/glossary.md#access-level-list) to define hierarchical access levels for cluster-wide operations.

For operations where both [access control lists](../concepts/glossary.md#access-control-list) and [access level lists](../concepts/glossary.md#access-level-list) are checked, both mechanisms are applied together: the action is available only if both checks allow it, and unavailable if at least one check fails. For other operations, only the corresponding check mechanism is applied.

### Access level hierarchy

Access level lists form a hierarchy used in [{{ ydb-ui-name }}](../reference/ydb-ui/ydb-monitoring.md), viewer, and many other cluster-wide actions (ordered from least to most privileges):

- `database_allowed_sids` (`Database`) - access to operations in the context of a specific database.
- `viewer_allowed_sids` (`Viewer`) - access to viewing cluster-wide state.
- `monitoring_allowed_sids` (`Monitoring`) - access to operational actions in {{ ydb-ui-name }}.
- `administration_allowed_sids` (`Administration`) - administrative actions on the cluster and databases.

A higher level automatically includes all lower ones, so a subject only needs to be present in one list. For example, being in `administration_allowed_sids` automatically grants privileges `monitoring`, `viewer`, and `database`.
Details on each level are in the section [Access level description](#access-level-descriptions).

Additionally, there are two separate access level lists for specific operations:

- `bootstrap_allowed_sids` — allows cluster initialization operations.
- `register_dynamic_node_allowed_sids` — allows node registration in the cluster.

### Description of access levels {#access-level-descriptions}

Access level lists are configured in the [security configuration](../reference/configuration/security_config.md#security-access-levels) and define privileges for:

- **Database** (included in `database_allowed_sids`) — access only in the context of a specific database. You can open {{ ydb-ui-name }} and work with the data of this database, but you cannot run cluster-wide queries (for example, view the list of cluster nodes). Queries without specifying a database are prohibited.
- **Viewer** (included in `viewer_allowed_sids`) — read-only access to the cluster-wide state: you can view [{{ ydb-ui-name }}](../reference/ydb-ui/ydb-monitoring.md) pages and diagnostic information, but you cannot run actions that change the system state.
- **Monitoring** (included in `monitoring_allowed_sids`) — access to operational actions in {{ ydb-ui-name }}, including actions that can change the system state. For example, starting a backup, restoring a database, or running YQL queries through {{ ydb-ui-name }}.
- **Administration** (included in `administration_allowed_sids`) — grants the right to perform administrative actions on databases or the cluster. Full administrative access to the cluster and its databases. Also used for changing configuration, schema operations that require administrative rights, and other administrative checks.
- **Register node** (included in `register_dynamic_node_allowed_sids`) — a separate (non-hierarchical) level for registering dynamic nodes in the cluster. It does not automatically grant `database`/`viewer`/`monitoring`/`administration` rights. For technical reasons, if the list is specified (not empty), it must include `root@builtin`.
- **Bootstrap** (included in `bootstrap_allowed_sids`) — a separate (non-hierarchical) level only for cluster initialization operations. Used in an uninitialized state when the authentication subsystem is not yet functioning. Initialization is allowed if the subject is in `bootstrap_allowed_sids` or `administration_allowed_sids`, while `bootstrap` itself does not grant full administrative privileges.
>>>>>>> 3269492a60d (docs: replace remaining Embedded UI mentions with YDB UI preset (#54685))

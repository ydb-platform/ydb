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

The owner of an object can be viewed using the CLI command [`describe`](../reference/ydb-cli/commands/scheme-describe.md).

## Access level lists {#access-level-lists}

In addition to [access control lists](../concepts/glossary.md#access-control-list) that control access to specific [scheme objects](../concepts/glossary.md#scheme-object), {{ ydb-short-name }} uses [access level lists](../concepts/glossary.md#access-level-list) to define hierarchical access levels for cluster-wide operations.

For operations where both [access control lists](../concepts/glossary.md#access-control-list) and [access level lists](../concepts/glossary.md#access-level-list) are checked, both checks are applied together: an action is allowed only if both checks allow it, and denied if either check fails. For other operations, only the corresponding check mechanism is used.

### Hierarchy of access levels

Access level lists form a hierarchy (used by the [Embedded UI](../reference/embedded-ui/ydb-monitoring.md), viewer, and many other cluster-wide actions; ordered from lower to higher privileges):

- `database_allowed_sids` (`Database`) - access to operations within a specific database scope
- `viewer_allowed_sids` (`Viewer`) - read-only access to cluster-wide state
- `monitoring_allowed_sids` (`Monitoring`) - access to operational actions in Embedded UI
- `administration_allowed_sids` (`Administration`) - administrative actions on the cluster and databases

Higher levels automatically include all lower level privileges, so a subject only needs to appear in one list. For example, presence in `administration_allowed_sids` automatically grants `monitoring`, `viewer`, and `database` privileges.
See [Access level descriptions](#access-level-descriptions) for details.

Additionally, there are two separate access level lists for specific operations:

- `bootstrap_allowed_sids` — allows cluster bootstrap operations
- `register_dynamic_node_allowed_sids` — allows node registration in the cluster

### Access level descriptions {#access-level-descriptions}

Access level lists are configured in the [security configuration](../reference/configuration/security_config.md#security-access-levels) and determine privileges for:

- **Database** (presence in `database_allowed_sids`) — access only within a specific database scope. Subjects can open Embedded UI and work with that database data, but cannot run cluster-wide requests (for example, listing cluster nodes). Requests without a specified database are forbidden.
- **Viewer** (presence in `viewer_allowed_sids`) — read-only access to cluster-wide state: subjects can open [Embedded UI](../reference/embedded-ui/ydb-monitoring.md) pages and diagnostics, but cannot run actions that change system state.
- **Monitoring** (presence in `monitoring_allowed_sids`) — access to operational actions in Embedded UI, including actions that can change system state. For example, backup, database restore, or executing YQL statements from Embedded UI.
- **Administration** (presence in `administration_allowed_sids`) — Full administrative access to the cluster and its databases. Also used for config changes, scheme operations requiring admin privileges, and other administrative checks.
- **Register node** (presence in `register_dynamic_node_allowed_sids`) — a separate (non-hierarchical) level for dynamic node registration in the cluster. It does not automatically grant `database`/`viewer`/`monitoring`/`administration` privileges. For technical reasons, if this list is configured (non-empty), it must include `root@builtin`.
- **Bootstrap** (presence in `bootstrap_allowed_sids`) — a separate (non-hierarchical) level only for cluster bootstrap operations. It is used in an uninitialized state, when the authentication subsystem is not yet functional. Bootstrap is allowed if the subject is in `bootstrap_allowed_sids` or `administration_allowed_sids`; `bootstrap` by itself does not grant full administrative privileges.


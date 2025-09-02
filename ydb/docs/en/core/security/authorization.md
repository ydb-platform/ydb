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

* [Ansible](../devops/deployment-options/ansible/initial-deployment.md)
* [Kubernetes](../devops/deployment-options/kubernetes/initial-deployment.md)
* [Manually](../devops/deployment-options/manual/initial-deployment.md)

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

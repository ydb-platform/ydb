# Access management

{{ ydb-short-name }} supports authentication by username and password.

## Built-in groups {#builtin}

A {{ ydb-short-name }} cluster has built-in groups that offer predefined sets of roles:

 Group | Description
--- | ---
 `ADMINS` | Unlimited rights for the entire cluster schema.
 `DATABASE-ADMINS` | Rights to create and delete databases (`CreateDatabase`, `DropDatabase`).
 `ACCESS-ADMINS` | Rights to manage rights of other users (`GrantAccessRights`).
 `DDL-ADMINS` | Rights to alter the database schema (`CreateDirectory`, `CreateTable`, `WriteAttributes`, `AlterSchema`, `RemoveSchema`).
 `DATA-WRITERS` | Rights to change data (`UpdateRow`, `EraseRow`).
 `DATA-READERS` | Rights to read data (`SelectRow`).
 `METADATA-READERS` | Rights to read metadata without accessing data (`DescribeSchema` and `ReadAttributes`).
 `USERS` | Rights to connect to databases (`ConnectDatabase`).

All users are added to the `USERS` group by default. The `root` user is added to the `ADMINS` group by default.

You can see how groups inherit permissions below. For example, the `DATA-WRITERS` group includes all the permissions from `DATA-READERS`:

![groups](../_assets/groups.svg)

## Manage groups {#groups}

To create, update, or delete a group, use the YQL operators:

* [{#T}](../yql/reference/syntax/create-group.md).
* [{#T}](../yql/reference/syntax/alter-group.md).
* [{#T}](../yql/reference/syntax/drop-group.md).

{% note info %}

When using the names of built-in groups in the `ALTER GROUP` commands, those names must be provided in the upper case. In addition, the names of built-in groups containing the "-" symbol must be surrounded with the backticks, for example:

```
ALTER GROUP `DATA-WRITERS` ADD USER myuser1;
```

{% endnote %}

## Managing users {#users} 

To create, update, or delete a user, use the YQL operators:

* [{#T}](../yql/reference/syntax/create-user.md).
* [{#T}](../yql/reference/syntax/alter-user.md).
* [{#T}](../yql/reference/syntax/drop-user.md).

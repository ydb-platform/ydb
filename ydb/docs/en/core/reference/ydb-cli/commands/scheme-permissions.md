# Permissions

## General list of commands

You can get a list of available commands via interactive help:

```bash
{{ ydb-cli }} scheme permissions --help
```

```text
Usage: ydb [global options...] scheme permissions [options...] <subcommand>

Description: Modify permissions

Subcommands:
permissions                 Modify permissions
├─ chown                    Change owner
├─ clear                    Clear permissions
├─ grant                    Grant permission (aliases: add)
├─ list                     List permissions
├─ revoke                   Revoke permission (aliases: remove)
├─ set                      Set permissions
├─ clear-inheritance        Do not inherit permissions from the parent
└─ set-inheritance          Inherit permissions from the parent
```

All commands have an additional parameter, which is not critical for them:
`--timeout ms` - a technical parameter that sets the server response timeout.

## grant, revoke

The `grant` and `revoke` commands allow you to establish and revoke, respectively, access rights to schema objects for a user or group of users. Essentially, they are analogues of the corresponding YQL [GRANT](../../../yql/reference/syntax/grant.md) and [REVOKE](../../../yql/reference/syntax/revoke.md) commands.

The syntax of the {{ ydb-short-name }} CLI commands is as follows:

```bash
{{ ydb-cli }} [connection options] scheme permissions grant  [options...] <path> <subject>
{{ ydb-cli }} [connection options] scheme permissions revoke [options...] <path> <subject>
```

Parameters:

`<path>` — the full path from the root of the cluster to the object whose rights need to be modified.
`<subject>` — the name of the user or group whose access rights are being changed.

Additional parameters `[options...]`:

`{-p|--permission} NAME` — the list of rights that need to be granted (`grant`) or revoked (`revoke`) for the user.

Each right must be passed as a separate parameter, for example:

```bash
{{ ydb-cli }} scheme permissions grant -p "ydb.access.grant" -p "ydb.generic.read" '/Root/db1/MyApp/Orders' testuser 
```

## set

The `set` command allows you to set access rights to schema objects for a user or group of users.

Command syntax:

```bash
{{ ydb-cli }} [connection options] scheme permissions set  [options...] <path> <subject>
```

The values of all parameters are identical to the [`grant`, `revoke`](#grant-revoke) commands. However, the key difference of the `set` command from `grant` and `revoke` is that it sets exactly those access rights to the specified object that are listed in the `-p (--permission)` parameters. Other rights for the specified user or group will be revoked.

For example, previously the user `testuser` was granted rights to the object `'/Root/db1'` such as `"ydb.granular.select_row"`, `"ydb.granular.update_row"`, `"ydb.granular.erase_row"`, `"ydb.granular.read_attributes"`, `"ydb.granular.write_attributes"`, `"ydb.granular.create_directory"`.
Then, as a result of executing the command, all rights to the specified object will be revoked (as if `revoke` was called for each of the rights) and only the right `"ydb.granular.select_row"` specified in the `set` command will remain:

```bash
{{ ydb-cli }} scheme permissions set -p "ydb.granular.select_row" '/Root/db1' testuser
```

## list

The `list` command allows you to obtain the current list of access rights to schema objects.

Command syntax:

```bash
{{ ydb-cli }} [connection options] scheme permissions list [options...] <path>
```

Parameters:
`<path>` — the full path from the cluster's root to the object you want to get rights for.

Example result of executing `list`:

```bash
{{ ydb-cli }} scheme permissions list '/Root/db1/MyApp'
```

```bash
Owner: root

Permissions:
user1:ydb.generic.read

Effective permissions:
USERS:ydb.database.connect
METADATA-READERS:ydb.generic.list
DATA-READERS:ydb.granular.select_row
DATA-WRITERS:ydb.tables.modify
DDL-ADMINS:ydb.granular.create_directory,ydb.granular.write_attributes,ydb.granular.create_table,ydb.granular.remove_schema,ydb.granular.alter_schema
ACCESS-ADMINS:ydb.access.grant
DATABASE-ADMINS:ydb.generic.manage
user1:ydb.generic.read
```

The result structure consists of three blocks:

- `Owner` — shows the owner of the schema object.
- `Permissions` — displays a list of rights directly given to this object.
- `Effective permissions` — displays a list of rights that are effectively applied to this schema object, taking into account the rules of rights inheritance. This list also includes all the rights displayed in the `Permissions` section.

## clear

The `clear` command allows you to revoke all previously granted rights to the schema object. Rights that apply to it by inheritance rules will continue to apply.

```bash
{{ ydb-cli }} [global options...] scheme permissions clear [options...] <path>
```

Parameters:
`<path>` — the full path from the root of the cluster to the object whose permissions need to be revoked.

For example, if you execute the command over the database state from the previous example [`list`](#list):

```bash
{{ ydb-cli }} scheme permissions clear '/Root/db1/MyApp' 
```

And then execute the `list` command again on the object `/Root/db1/MyApp`, you will get the following result:

```bash
Owner: root

Permissions:
none

Effective permissions:
USERS:ydb.database.connect
METADATA-READERS:ydb.generic.list
DATA-READERS:ydb.granular.select_row
DATA-WRITERS:ydb.tables.modify
DDL-ADMINS:ydb.granular.create_directory,ydb.granular.write_attributes,ydb.granular.create_table,ydb.granular.remove_schema,ydb.granular.alter_schema
ACCESS-ADMINS:ydb.access.grant
DATABASE-ADMINS:ydb.generic.manage
```

Note that the `Permissions` section is now empty. This means all permissions for this object have been revoked. Also, there have been changes in the contents of the `Effective permissions` section: it no longer lists the permissions that were granted directly to the object `/Root/db1/MyApp`.

## chown

The `chown` command allows you to change the owner of a schema object.

Command syntax:

```bash
{{ ydb-cli }} [connection options] scheme permissions chown [options...] <path> <owner>
```

Parameters:
`<path>` — the full path from the root of the cluster to the object whose permissions need to be modified.
`<owner>` — the name of the new owner (a user or a group) of the specified object.

Example of a `chown` command:

```bash
{{ ydb-cli }} scheme permissions chown '/Root/db1' testuser
```

{% note info %}

In the current version of {{ ydb }}, there is a restriction that only the user who is the current owner of the schema object can change the owner.

{% endnote %}

## clear-inheritance

The `clear-inheritance` command allows you to prohibit the inheritance of permissions for a schema object.

Command syntax:

```bash
{{ ydb-cli }} [connection options] scheme permissions clear-inheritance [options...] <path>
```

Parameters:
`<path>` — the full path from the cluster' root to the object whose permissions need to be modified.

Example of a `clear-inheritance` command:

```bash
{{ ydb-cli }} scheme permissions clear-inheritance '/Root/db1'
```

## set-inheritance

The `set-inheritance` command allows you to enable permission inheritance for a schema object.

Command syntax:

```bash
{{ ydb-cli }} [connection options] scheme permissions set-inheritance [options...] <path>
```

Parameters:
`<path>` — the full path from the cluster's root to the object whose permissions need to be modified.

Example of a `set-inheritance` command:

```bash
{{ ydb-cli }} scheme permissions set-inheritance '/Root/db1'
```

# Schema operations

Schema (or DDL) operation logging makes it possible to monitor changes to schema entities made by users.

Logged operations include:
- creating, modifying and deleting database objects, tables, directories, connections to external databases and the likes,
- modifying schema entities access rights (ACL),
- creating, modifying and deleting users and groups for [login and password authentication](../concepts/auth.md#static-credentials) mode.

## Record attributes

| __Attribute__ | __Description__ |
|:----|:----|
| `component` | `schemeshard`.
| `remote_address` | IP address of the client who sent the request.
| `subject` | User SID (account name) of the user on whose behalf the operation is performed.
| `database` | Path of the database in which the operation is performed.
| `operation` | Operation type.
| `start_time` | Operation start time.
| `end_time` | Operation end time.
| `status` | General operation status: `SUCCESS` or `ERROR`.
| `detailed_status` | Internal operation status.
| `tx_id` | Unique transaction identifier.

### Operation specific attributes

Schema operations:
| __Kind__ | __Attribute__ | __Description__ |
|:----|:----|:----|
| _All_ ||
|| `paths` | List of paths in the database that are changed by the operation (for example, `[/root/db/table-a, /root/db/table-b]`).<br>Required. |
| _Ownership and permissions_ ||
|| `new_owner` | User SID of the new owner of the object when ownership is transferred.
|| `acl_add` | List of added permissions, in [short notation](./short-access-control-notation.md) (for example, `[+R:someuser]`).
|| `acl_remove` | List of revoked permissions, in [short notation](./short-access-control-notation.md) (for example, `[-R:somegroup]`).
| _[User (or custom) attributes](../concepts/datamodel/table?#users-attr)_ ||
|| `user_attrs_add` | List of custom attributes added when creating objects or updating attributes (for example, `[attr1: A, attr2: B]`).
|| `user_attrs_remove` | List of custom attributes removed when creating objects or updating attributes (for example, `[attr1, attr2]`).

[Credentials](../concepts/auth#static-credentials) management operations:
| __Attribute__ | __Description__ |
|:----|:----|
| `login_user` | User name (when [adding or modifying the user](./access-management.md#users)).
| `login_group` | Group name (when [adding or modifying the user group](./access-management.md#groups)).
| `login_member` | User name (when [adding or removing the user to or from a group](./access-management.md#groups)).

## How to enable

Audit log must be [enabled](audit-log.md#enabling-audit-log) on a cluster level.

Scheme operation logging does not require anything else.

## Things to know

- Logging occurs at the {{ ydb-short-name }} component `Scheme Shard`.

[//]: # (TODO: `start_time` and `end_time` mark start and end time of the operation.)
- Logging occurs when schemeshard accepts operation for an execution.

- Request to create schema object could generate a series of schema operations and could result in a multiple audit log records (one for each operation). All such records will have the same `tx_id`. This is the case when the object path contain intermediate elements which are also need to be created (for example, if path `root/a` is not exist, then request to create table `root/a/table` will be performed by two sequential operations: `create root/a` and then `create root/a/table`).

- If [authentication](../deploy/configuration/config#auth) on a cluster is disabled, requests will be logged, `subject` value will be `{none}`.

## Record examples

```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "DROP TABLE",
    "tx_id": "845026768199165",
    "paths": "[/root/db/table1  ]",
    "status": "SUCCESS",
    "detailed_status": "StatusAccepted"
}
```
```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "bfbohb360qqqql1ef604@ad",
    "database": "/root/db",
    "operation": "CREATE DIRECTORY",
    "tx_id": "563346945792517",
    "paths": "[/root/db/default-exports]",
    "status": "SUCCESS",
    "detailed_status": "StatusAlreadyExists",
    "reason": "Check failed: path: '/root/db/default-exports', error: path exist, request accepts it (id: [...], type: EPathTypeDir, state: EPathStateNoChanges)"
}
```
```json
{
    "component": "schemeshard",
    "remote_address": "xxxx:xxx:xxxx:xx:xxxx:xxxx:xxxx:xxx",
    "subject": "{none}",
    "database": "/root/db1",
    "operation": "MODIFY ACL",
    "tx_id": "281474976775658",
    "paths": "[/root/db/some_dir]",
    "acl_add": "[+(ConnDB):subject:-]",
    "status": "SUCCESS",
    "detailed_status": "StatusAccepted"
}
```

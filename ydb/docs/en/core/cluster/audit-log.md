# Audit log

{% include [release-candidate](../_includes/trunk.md) %}

An _audit log_ is a file that includes data about all the operations that tried to change the {{ ydb-short-name }} objects and ACLs, successfully or unsuccessfully, such as:

* Creating, updating, and deleting databases.
* Creating and deleting directories.
* Creating or editing database scheme, changing the number of partitions, backup and recovery, copying and renaming, and deleting tables.
* Creating, editing, or deleting topics.
* Changing ACLs.

The file is written on each {{ ydb-short-name }} cluster node. You can access your audit log only from a Terminal session.

## Audit log events {#events}

The information about each operation is saved to the audit log as a separate event. Each event includes a set of attributes. Some of those attributes describe the operation, others describe the transaction, within which the operation was executed. If a transaction included multiple operations, its attributes for such events will be the same.

The operation attributes are as follows:

* `operation` (required): Name of the operation.
* `path` (optional): Path to the changed object.
* `src path` (optional): Path to the source object used for copy and move operations. This field may include multiple values.
* `dst path` (optional): Path to the target object used for copy and move operations. This field may include multiple values.
* `set owner` (optional): New owner assigned when changing the ACL.
* `add access` (optional): Access added when changing the ACL. This field may include multiple values.
* `remove access` (optional): Access removed when changing the ACL. This field may include multiple values.
* `protobuf request` (optional): Description of a schema or an ACL change in ProtoBuf format.

The transaction attributes are as follows:

* `txId` (required): Unique transaction ID.
* `database` (optional): Path to the database.
* `subject` (required): Event source SID in the `<login>@<subsystem>` format. Unless mandatory authentication is enabled, the key will have the `no subject` value.
* `status` (required): Transaction completion status.
* `reason` (optional): Error message.

The format of event records is defined by the `format` parameter in the [cluster configuration](#enabling-audit-log). Here is an example of events in `JSON` format:

```json
2022-12-05T18:58:39.517833Z: {"protobuf request":"WorkingDir: \"/my_dir/db1\" OperationType: ESchemeOpCreateTable CreateTable { Name: \"my_table\" Columns { Name: \"id\" Type: \"Uint64\" NotNull: false } Columns { Name: \"name\" Type: \"String\" NotNull: false } KeyColumnNames: \"id\" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: \"ssd\" } Log { PreferredPoolKind: \"ssd\" } Data { PreferredPoolKind: \"ssd\" } } } } } FailOnExist: false","txId":"281474976720657","subject":"no subject","status":"StatusAccepted","operation":"CREATE TABLE","path":"/my_dir/db1/my_table","database":"/my_dir/db1"}

2022-12-05T19:01:22.309877Z: {"dst path":"{/my_dir/db1/my_table2}","database":"/my_dir/db1","txId":"281474976720658","protobuf request":"OperationType: ESchemeOpMoveTable MoveTable { SrcPath: \"/my_dir/db1/my_table\" DstPath: \"/my_dir/db1/my_table2\" }","status":"StatusAccepted","subject":"no subject","src path":"{/my_dir/db1/my_table}","operation":"ALTER TABLE RENAME"}
```

The same events in `TXT` format will look as follows:

```txt
2022-12-05T18:58:39.517833Z: txId=281474976720657, database=/my_dir/db1, subject=no subject, status=StatusAccepted, operation=CREATE TABLE, path=/my_dir/db1/my_table, protobuf request=WorkingDir: "/my_dir/db1" OperationType: ESchemeOpCreateTable CreateTable { Name: "my_table" Columns { Name: "id" Type: "Uint64" NotNull: false } Columns { Name: "name" Type: "String" NotNull: false } KeyColumnNames: "id" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: "ssd" } Log { PreferredPoolKind: "ssd" } Data { PreferredPoolKind: "ssd" } } } } } FailOnExist: false

2022-12-05T19:01:22.309877Z: txId=281474976720658, database=/my_dir/db1, subject=no subject, status=StatusAccepted, operation=ALTER TABLE RENAME, src path={/my_dir/db1/my_table}, dst path={/my_dir/db1/my_table2}, protobuf request=OperationType: ESchemeOpMoveTable MoveTable { SrcPath: "/my_dir/db1/my_table" DstPath: "/my_dir/db1/my_table2" }
```

## Enabling audit log {#enabling-audit-log}

Saving events to the audit log is enabled at the cluster level. To enable this feature, add the `audit` section to the [cluster configuration](../deploy/configuration/config.md) file:

```proto
audit:
  audit_file_path: "path_to_log_file"
  format: JSON
```

| Parameter | Value |
--- | ---
| `audit_file_path` | Path to the file the audit log will be saved to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it.<br>This parameter is optional. Make sure to specify either `audit_file_path` or `log_name`, or both. |
| `format` | Audit log format.<br>The acceptable values are:<ul><li>`JSON`: Serialized [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}.</li><li>`TXT`: Text format.</ul> |

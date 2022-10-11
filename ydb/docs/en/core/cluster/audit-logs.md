# Audit logs

All schema changes (successful and unsuccessful) and ACL changes are recorded in _audit logs_.

## Enabling audit logs

Audit logs are provided as part of YDB [component logging](./logs.md).

To enable audit logs, you need to [change the logging level](../maintenance/embedded_monitoring/logs.md#change_log_level):

1. Follow the link in the format

   ```bash
   http://<endpoint>:8765/cms
   ```

   The `Cluster Management System` page opens.

1. On the **Configs** tab, click on the `LogConfigItems` line.

1. Under `Component log settings`, find the `FLAT_TX_SCHEMESHARD` component. Set this component's logging level to `NOTICE` or higher.

1. To save changes, click `Submit`

Audit logs are written together with other YDB logs.

## Audit log format {#format}

A log event consists of comma-separated `key: value` fields:

```text
2022-08-03T22:41:43.860439Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710670, database: /Root, subject: no subject, status: StatusSuccess, operation: MODIFY ACL, path: Root, add access: +(CT):user0@builtin, protobuf request: WorkingDir: "" OperationType: ESchemeOpModifyACL ModifyACL { Name: "Root" DiffACL: "\n\031\010\000\022\025\010\001\020@\032\ruser0@builtin \003" }

2022-08-03T22:41:43.931561Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710672, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: DROP TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpDropTable Drop { Name: "KeyValue" }

2022-08-03T22:41:43.895591Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710671, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: CREATE DIRECTORY, path: /Root/Test1234, protobuf request: WorkingDir: "/Root" OperationType: ESchemeOpMkDir MkDir { Name: "Test1234" } FailOnExist: true, operation: CREATE TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpCreateTable CreateTable { Name: "KeyValue" Columns { Name: "Key" Type: "Uint32" NotNull: false } Columns { Name: "Value" Type: "String" NotNull: false } KeyColumnNames: "Key" PartitionConfig { ColumnFamilies { Id: 0 StorageConfig { SysLog { PreferredPoolKind: "test" } Log { PreferredPoolKind: "test" } Data { PreferredPoolKind: "test" } } } } } FailOnExist: false
```

One event describes one transaction. An event can describe several operations performed within a single transaction. In this case, some of the fields will describe [transaction events](#tx-fields) and some of the fields will describe [operation events](#sub-operation-fields) within a transaction.

### Transaction fields {#tx-fields}

* `txId`: (mandatory) The unique transaction ID.
* `database`: (optional) The path to the database.
* `subject`: (mandatory) The event source SID (`<login>@<subsystem>` format). If not specified, the value is `no subject`.
* `status`: (mandatory) The transaction completion status.
* `reason`: (optional) An error message.

### Operation fields {#sub-operation-fields}

* `operation`: (mandatory) The operation name.
* `path`: (optional) The path to the change object. This field might change during a transaction.
* `src path`: (optional) The path to the source object (for copy and move operations).
* `dst path`: (optional) The path to the target object (for copy and move operations).
* `no path`: (optional) If there is no change object, the value is `no path`.
* `set owner`: (optional) The new owner when changing ACL.
* `add access`: (optional) Add access when changing ACL. The field can be repeated.
* `remove access`: (optional) Remove access when changing ACL. The field can be repeated.
* `protobuf request`: (optional) A description of a schema or ACL change in protobuf format.

<!--
### <a name="statuses"></a>List of possible statuses
- StatusSuccess
- StatusAccepted
- StatusPathDoesNotExist
- StatusPathIsNotDirectory
- StatusAlreadyExists
- StatusSchemeError
- StatusNameConflict
- StatusInvalidParameter
- StatusMultipleModifications
- StatusReadOnly
- StatusTxIdNotExists
- StatusTxIsNotCancellable
- StatusAccessDenied
- StatusNotAvailable
- StatusPreconditionFailed
- StatusRedirectDomain
- StatusQuotaExceeded
- StatusResourceExhausted

### <a name="names"></a>List of possible operations
- CREATE TABLE
- CREATE DIRECTORY
- CREATE PERSISTENT QUEUE
- DROP TABLE
- DROP PERSISTENT QUEUE
- ALTER TABLE
- ALTER PERSISTENT QUEUE
- MODIFY ACL
- DROP DIRECTORY
- ALTER TABLE PARTITIONS
- BACKUP TABLE
- CREATE DATABASE
- DROP DATABASE
- CREATE RTMR VOLUME
- CREATE BLOCK STORE VOLUME
- ALTER BLOCK STORE VOLUME
- ALTER BLOCK STORE VOLUME ASSIGN
- DROP BLOCK STORE VOLUME
- CREATE KESUS
- DROP KESUS
- DROP DATABASE
- CREATE SOLOMON VOLUME
- DROP SOLOMON VOLUME
- ALTER KESUS
- ALTER DATABASE
- ALTER USER ATTRIBUTES
- DROP PATH UNSAFE
- CREATE TABLE WITH INDEXES
- CREATE INDEX
- CREATE TABLE COPY FROM
- DROP INDEX
- CREATE DATABASE
- ALTER DATABASE
- DROP DATABASE
- ESchemeOp_DEPRECATED_35
- ALTER DATABASE MIGRATE
- ALTER DATABASE MIGRATE DECISION
- BUILD INDEX
- ALTER TABLE BUILD INDEX INIT
- ALTER TABLE LOCK
- ALTER TABLE BUILD INDEX APPLY
- ALTER TABLE BUILD INDEX FINISH
- ALTER INDEX
- ALTER SOLOMON VOLUME
- ALTER TABLE UNLOCK
- ALTER TABLE BUILD INDEX FINISH
- ALTER TABLE BUILD INDEX INIT
- ALTER TABLE DROP INDEX
- ALTER TABLE DROP INDEX
- ALTER TABLE BUILD INDEX CANCEL
- CREATE FILE STORE
- ALTER FILE STORE
- DROP FILE STORE
- RESTORE TABLE
- CREATE COLUMN STORE
- ALTER COLUMN STORE
- DROP COLUMN STORE
- CREATE COLUMN TABLE
- ALTER COLUMN TABLE
- DROP COLUMN TABLE
- ALTER LOGIN
- ATER TABLE CREATE CDC STREAM
- CREATE CDC STREAM
- ATER TABLE CREATE CDC STREAM
- ATER CDC STREAM
- ATER CDC STREAM
- ATER TABLE ATER CDC STREAM
- DROP CDC STREAM
- DROP CDC STREAM
- ATER TABLE DROP CDC STREAM
- ALTER TABLE RENAME
- CREATE SEQUENCE
- ALTER SEQUENCE
- DROP SEQUENCE
- CREATE REPLICATION
- ALTER REPLICATION
- DROP REPLICATION
- CREATE BLOB DEPOT
- ALTER BLOB DEPOT
- DROP BLOB DEPOT
- ALTER TABLE INDEX RENAME -->

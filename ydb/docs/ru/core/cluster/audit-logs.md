# Аудитные логи

Информация обо всех изменениях схемы (успешных и неуспешных), а также об изменениях ACL записывается в _аудитные логи_.

## Формат аудитных логов {#format}

Событие лога состоит из полей `ключ: значение`, разделенных запятыми:

```text
2022-08-03T22:41:43.860439Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710670, database: /Root, subject: no subject, status: StatusSuccess, operation: MODIFY ACL, path: Root, add access: +(CT):user0@builtin, protobuf request: WorkingDir: "" OperationType: ESchemeOpModifyACL ModifyACL { Name: "Root" DiffACL: "\n\031\010\000\022\025\010\001\020@\032\ruser0@builtin \003" }

2022-08-03T22:41:43.931561Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710672, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: DROP TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpDropTable Drop { Name: "KeyValue" }

2022-08-03T22:41:43.895591Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710671, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: CREATE DIRECTORY, path: /Root/Test1234, protobuf request: WorkingDir: "/Root" OperationType: ESchemeOpMkDir MkDir { Name: "Test1234" } FailOnExist: true, operation: CREATE TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpCreateTable CreateTable { Name: "KeyValue" Columns { Name: "Key" Type: "Uint32" NotNull: false } Columns { Name: "Value" Type: "String" NotNull: false } KeyColumnNames: "Key" PartitionConfig { ColumnFamilies { Id: 0 StorageConfig { SysLog { PreferredPoolKind: "test" } Log { PreferredPoolKind: "test" } Data { PreferredPoolKind: "test" } } } } } FailOnExist: false
```

Одно событие описывает одну транзакцию. В событии может быть описано несколько операций, выполненных в рамках одной транзакции. В этом случае часть полей будут описывать [события транзакции](#tx-fields), а часть полей — [события операций](#sub-operation-fields) внутри транзакции.

### Поля транзакции {#tx-fields}

* `txId` — (обязательно) уникальный идентификатор транзакции.
* `database` — (опционально) путь к базе данных.
* `subject` — (обязательно) SID источника события (формат `<login>@<subsystem>`). Если не определен, значение `no subject`.
* `status` — (обязательно) статус завершения транзакции.
* `reason` — (опционально) сообщение об ошибке.

### Поля операции {#sub-operation-fields}

* `operation` — (обязательно) название операции.
* `path` — (опционально) путь к объекту изменения.
* `src path` — (опционально) путь к исходному объекту (для операций копирования и перемещения).
* `dst path` — (опционально) путь к конечному объекту (для операций копирования и перемещения).
* `no path` — (опционально) если объект изменения отсутствует, содержит значение `no path`.
* `set owner` — (опционально) новый владелец при изменении ACL.
* `add access` — (опционально) добавление доступа при изменении ACL. Поле может повторятся.
* `remove access` — (опционально) удаление доступа при изменении ACL. Поле может повторяться.
* `protobuf request` — (опционально) Описание изменения схемы или ACL в формате protobuf.

<!-- 
### <a name="statuses"></a>Список возможных статусов
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

### <a name="names"></a>Список возможных операций
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

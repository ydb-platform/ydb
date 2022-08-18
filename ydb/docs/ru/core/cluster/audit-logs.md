# Аудитные логи
В аудитные логи пишется информация обо всех изменениях схемы (успешных и неуспешных) и обо всех изменениях ACL. Аудитные логи записывают в логи `SchemeShard` с пометкой `FLAT_TX_SCHEMESHARD NOTICE: AUDIT:`

### Пример логов
```
2022-08-03T22:41:43.860439Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710670, database: /Root, subject: no subject, status: StatusSuccess, operation: MODIFY ACL, path: Root, add access: +(CT):user0@builtin, protobuf request: WorkingDir: "" OperationType: ESchemeOpModifyACL ModifyACL { Name: "Root" DiffACL: "\n\031\010\000\022\025\010\001\020@\032\ruser0@builtin \003" }

2022-08-03T22:41:43.931561Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710672, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: DROP TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpDropTable Drop { Name: "KeyValue" }

2022-08-03T22:41:43.895591Z node 1 :FLAT_TX_SCHEMESHARD NOTICE: AUDIT: txId: 281474976710671, database: /Root, subject: user0@builtin, status: StatusAccepted, operation: CREATE DIRECTORY, path: /Root/Test1234, protobuf request: WorkingDir: "/Root" OperationType: ESchemeOpMkDir MkDir { Name: "Test1234" } FailOnExist: true, operation: CREATE TABLE, path: /Root/Test1234/KeyValue, protobuf request: WorkingDir: "/Root/Test1234" OperationType: ESchemeOpCreateTable CreateTable { Name: "KeyValue" Columns { Name: "Key" Type: "Uint32" NotNull: false } Columns { Name: "Value" Type: "String" NotNull: false } KeyColumnNames: "Key" PartitionConfig { ColumnFamilies { Id: 0 StorageConfig { SysLog { PreferredPoolKind: "test" } Log { PreferredPoolKind: "test" } Data { PreferredPoolKind: "test" } } } } } FailOnExist: false
```

### Формат аудитных логов
Аудитные логи пишутся в формате пар `ключ: значение`. Каждая такая пара отделяется запятой. В каждой записи может быть описано несколько подопераций. Из-за этого часть полей в аудитных логах будут принадлежать всей транзакции - они пишутся один раз, другие поля принадлежат конкретным подоперациям и могут повторяться в зависимости от количества подопераций

##### Поля транзакции
- `txId` - *(required)* уникальный идентификатор транзакции
- `database` - *(optional)* путь к базе данных 
- `subject` - *(required)* SID инициатора (формат `<login>@<subsystem>`). если не определен, значение `no subject`
- `status` - *(required)* статус выполнения транзакции. ниже [список](#statuses) всевозможных статусов
- `reason` - *(optional)* причина ошибки, если что-то пошло не так

##### Поля подоперации
- `operation` - *(required)* название операции. ниже [список](#names) всевозможных операций
- `path` - *(optional)* путь к объекту изменения
- `src path` - *(optional)* путь к исходному объекту. для операций копирования/перемещения
- `dst path` - *(optional)* путь к конечному объекту. для операций копирования/перемещения
- `no path` - *(optional)* если объект изменения отсутствует, в лог пишется статус `no path`. у этого поля нет значения
- `set owner` - *(optional)* новый владелец, при изменении ACL
- `add access` - *(repeated)* добавление доступа, при изменении ACL
- `remove access` - *(repeated)* удаление доступа, при изменении ACL
- `protobuf request` - *(optional)* протобуф на изменение схемы или ACL

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
- ALTER TABLE INDEX RENAME

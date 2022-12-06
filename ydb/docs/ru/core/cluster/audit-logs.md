# Аудитные логи

Информация обо всех изменениях схемы (успешных и неуспешных), а также об изменениях ACL записывается в _аудитные логи_.

## Подключение аудитных логов

Чтобы подключить аудитные логи, необходимо в файле отвечающем за конфигурацию кластера прописать:
```
audit:
  audit_file_path: "/Berkanavt/kikimr/audit.log"
  format: JSON
```
* `audit_file_path` - путь к файлу, куда будут писаться аудитные логи
* `format` - формат записи аудитных файлов (возможные значения: JSON или TXT)

## Формат аудитных логов {#format}

Событие лога состоит из полей `ключ: значение`. Конкретный формат записи полей зависит от значения `format` в cluster.yaml:

###### Запись в формате JSON
```
2022-12-05T18:58:39.517833Z: {"protobuf request":"WorkingDir: \"/ydb_vla_dev07/db1\" OperationType: ESchemeOpCreateTable CreateTable { Name: \"my_table\" Columns { Name: \"id\" Type: \"Uint64\" NotNull: false } Columns { Name: \"name\" Type: \"String\" NotNull: false } KeyColumnNames: \"id\" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: \"ssd\" } Log { PreferredPoolKind: \"ssd\" } Data { PreferredPoolKind: \"ssd\" } } } } } FailOnExist: false","txId":"281474976720657","subject":"no subject","status":"StatusAccepted","operation":"CREATE TABLE","path":"/ydb_vla_dev07/db1/my_table","database":"/ydb_vla_dev07/db1"}

2022-12-05T19:01:22.309877Z: {"dst path":"{/ydb_vla_dev07/db1/my_table2}","database":"/ydb_vla_dev07/db1","txId":"281474976720658","protobuf request":"OperationType: ESchemeOpMoveTable MoveTable { SrcPath: \"/ydb_vla_dev07/db1/my_table\" DstPath: \"/ydb_vla_dev07/db1/my_table2\" }","status":"StatusAccepted","subject":"no subject","src path":"{/ydb_vla_dev07/db1/my_table}","operation":"ALTER TABLE RENAME"}
```

###### Запись в формате TXT
```
2022-12-05T18:44:19.831251Z: txId=562949953441313, database=/ydb_vla_dev07/db1, subject=no subject, status=StatusAccepted, operation=CREATE TABLE, path=/ydb_vla_dev07/db1/my_table, protobuf request=WorkingDir: "/ydb_vla_dev07/db1" OperationType: ESchemeOpCreateTable CreateTable { Name: "my_table" Columns { Name: "id" Type: "Uint64" NotNull: false } Columns { Name: "name" Type: "String" NotNull: false } KeyColumnNames: "id" PartitionConfig { PartitioningPolicy { SizeToSplit: 2147483648 } ColumnFamilies { StorageConfig { SysLog { PreferredPoolKind: "ssd" } Log { PreferredPoolKind: "ssd" } Data { PreferredPoolKind: "ssd" } } } } } FailOnExist: false

2022-12-05T18:45:32.790052Z: txId=562949953441314, database=/ydb_vla_dev07/db1, subject=no subject, status=StatusAccepted, operation=ALTER TABLE RENAME, src path={/ydb_vla_dev07/db1/my_table}, dst path={/ydb_vla_dev07/db1/my_table2}, protobuf request=OperationType: ESchemeOpMoveTable MoveTable { SrcPath: "/ydb_vla_dev07/db1/my_table" DstPath: "/ydb_vla_dev07/db1/my_table2" }
```

Событие описаывает операцию. Несколько событий могут описывать несколько операций выполненных в рамках одной транзакции. В этом случае часть полей будут описывать [события транзакции](#tx-fields), а часть полей — [события операций](#sub-operation-fields) внутри транзакции.

### Поля операции {#tx-fields}

* `txId` — (обязательно) уникальный идентификатор транзакции.
* `database` — (опционально) путь к базе данных.
* `subject` — (обязательно) SID источника события (формат `<login>@<subsystem>`). Если не определен, значение `no subject`.
* `status` — (обязательно) статус завершения транзакции.
* `reason` — (опционально) сообщение об ошибке.

### Поля операции {#sub-operation-fields}

* `operation` — (обязательно) название операции.
* `path` — (опционально) путь к объекту изменения. Это поле может меняться в рамках транзакции.
* `src path` — (опционально) путь к исходному объекту (для операций копирования и перемещения). Поле может содержать несколько значений.
* `dst path` — (опционально) путь к конечному объекту (для операций копирования и перемещения). Поле может содержать несколько значений.
* `set owner` — (опционально) новый владелец при изменении ACL.
* `add access` — (опционально) добавление доступа при изменении ACL. Поле может содержать несколько значений.
* `remove access` — (опционально) удаление доступа при изменении ACL. Поле может содержать несколько значений.
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

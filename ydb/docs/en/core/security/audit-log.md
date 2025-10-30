# Audit log

_An audit log_ is a stream that includes data about all the operations that tried to change the {{ ydb-short-name }} objects, successfully or unsuccessfully. The audit log captures, among other things:

* Database: creating, editing, deleting databases and modifying database properties.
* Directory: Creating and deleting directories.
* Table: Creating or editing table schema, changing the number of partitions, running backup and recovery, copying and renaming, and deleting tables.
* Topic: Creating, editing, and deleting topics.
* ACL: Editing ACLs and transferring ownership.
* Configuration changes performed by {{ ydb-short-name }} components.
* DDL/DML requests.
* Authentication and administrative events.

The data of the audit log stream can be delivered to:

* File on each {{ ydb-short-name }} cluster node.
* Agent for delivering [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/) metrics.
* Standard error stream, `stderr`.

You can use any of the listed destinations or their combinations.

If you forward the stream to a file, access to the audit log is set by file-system rights. Saving the audit log to a file is recommended for production installations.

Forwarding the audit log to the standard error stream (`stderr`) is recommended for test installations. Further stream processing is determined by the {{ ydb-short-name }} cluster [logging](../devops/observability/logging.md) settings.

## Audit log events {#events}

Each operation in YDB is logged as a separate audit event.
YDB has multiple audit event sources - services or subsystems capable of generating such events.

Each audit event contains a set of attributes:
* Common attributes shared across all event sources.
* Source-specific attributes unique to the component that generated the record.

### Common attributes {#common-attributes}
Common attributes repeated across all audit sources.

#|
|| Attribute | Description ||
|| `subject` | Event source SID (`<login>@<subsystem>` format). Unless mandatory authentication is enabled, the attribute will be set to `{none}`.<br/>Required. ||
|| `sanitized_token` | A partially masked authentication token that was used to execute the request. Can be used to link related events while keeping the original credentials hidden. If authentication was not performed, the value will be `{none}`.<br/>Required. ||
|| `operation` | Operation name (for example, `ALTER DATABASE`, `CREATE TABLE`).<br/>Required. ||
|| `component` | Unique identifier of the audit event source that generated the event.<br/>Required. ||
|| `status` | Operation completion status.<br/>Acceptable values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li><li>`IN-PROCESS`: The operation is in progress.</li></ul>Required. ||
|| `reason` | Error message.<br/>Optional. ||
|| `request_id` | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context.<br/>Optional. ||
|| `remote_address` | The IP of the client that delivered the request.<br/>Optional. ||
|| `detailed_status` | Component-specific status value.<br/>Optional. ||
|| `database` | Database path (for example, `/my_dir/db`).<br/>Optional. ||
|| `cloud_id` | Cloud identifier of the {{ ydb-short-name }} database.<br/>Optional. ||
|| `folder_id` | Folder identifier of the {{ ydb-short-name }} cluster or database.<br/>Optional. ||
|| `resource_id` | Resource identifier of the {{ ydb-short-name }} database.<br/>Optional. ||
#|

### Schemeshard {#schemeshard}

* **Component value:** `schemeshard`.
* **Logged operations:** Schema operations triggered by DDL queries, ACL modifications, and user management operations.
* **How to enable:** Only basic audit configuration required.

#|
|| Attribute | Description ||
|| **Common schemeshard attributes** | > ||
|| `tx_id` | Transaction identifier for the operation.</br>Required. ||
|| `paths` | List of paths affected by the operation.</br>Optional. ||
|| **Ownership and permission attributes** | > ||
|| `new_owner` | New owner assigned to a resource.</br>Optional. ||
|| `acl_add` | Access control entries being added.</br>Optional. ||
|| `acl_remove` | Access control entries being removed.</br>Optional. ||
|| **Custom attributes** | > ||
|| `user_attrs_add` | User attributes being added to a resource.</br>Optional. ||
|| `user_attrs_remove` | User attributes being removed from a resource.</br>Optional. ||
|| **Login/Auth specific** | > ||
|| `login_user` | User name recorded by login operations.</br>Optional. ||
|| `login_group` | Group name recorded by login operations.</br>Optional. ||
|| `login_member` | Membership changes.</br>Optional. ||
|| `login_user_change` | Changes applied to user settings.</br>Optional. ||
|| `login_user_level` | Privilege level of the user recorded by audit events. Could be `admin`.</br>Optional. ||
|| **Import/Export operation attributes** | > ||
|| `id` | Unique identifier for export or import operations.</br>Optional. ||
|| `uid` | User-defined label for operations.</br>Optional. ||
|| `start_time` | Timestamp when an operation started.</br>Optional. ||
|| `end_time` | Timestamp when an operation completed.</br>Optional. ||
|| `last_login` | Timestamp of the user's last successful login.</br>Optional. ||
|| **Export-specific** | > ||
|| `export_type` | Export destination. Possible values: `yt`, `s3`.</br>Optional. ||
|| `export_item_count` | Number of exported items.</br>Optional. ||
|| `export_yt_prefix` | YT destination path prefix.</br>Optional. ||
|| `export_s3_bucket` | S3 bucket used for exports.</br>Optional. ||
|| `export_s3_prefix` | S3 destination prefix.</br>Optional. ||
|| **Import-specific** | > ||
|| `import_type` | Import source type. Value `s3`.</br>Optional. ||
|| `import_item_count` | Number of imported items.</br>Optional. ||
|| `import_s3_bucket` | S3 bucket used for imports.</br>Optional. ||
|| `import_s3_prefix` | S3 source prefix.</br>Optional. ||
#|

### GRPC services (Proxy) {#grpc-proxy}

* **Component value:** `grpc-proxy`.
* **Logged operations:** All non-internal gRPC requests.
* **Log classes:** Depends on the API: `Ddl`, `Dml`, `Operations`, `ClusterAdmin`, `DatabaseAdmin`, or other classes matching the endpoint.
* **Log phases:** `Received`, `Completed`.

#|
|| Attribute | Description ||
|| **Common gRPC attributes** | > ||
|| `grpc_method` | RPC method name.</br>Required.</br>Optional. ||
|| `request` | Sanitized representation of the incoming request.</br>Optional. ||
|| `start_time` | Operation start time in ISO 8601 format.</br>Required. ||
|| `end_time` | Operation end time in ISO 8601 format.</br>Optional. ||
|| **Transaction attributes** | > ||
|| `tx_id` | Transaction identifier.</br>Optional. ||
|| `begin_tx` | Flag set to `1` when the request starts a new transaction.</br>Optional. ||
|| `commit_tx` | Shows whether the request commits the transaction. Possible values: `true`, `false`.</br>Optional. ||
|| **Request fields** | > ||
|| `query_text` | Sanitized YQL query text.</br>Optional. ||
|| `prepared_query_id` | Identifier of a prepared query.</br>Optional. ||
|| `program_text` | TabletService `ExecuteTabletMiniKQL` payload.</br>Optional. ||
|| `schema_changes` | TabletService `ChangeTabletSchema` payload.</br>Optional. ||
|| `table` | Full table path (for example, for `BulkUpsert`).</br>Optional. ||
|| `row_count` | Number of rows affected by `BulkUpsert`.</br>Optional. ||
|| `tablet_id` | Tablet identifier.</br>Optional. ||
#|

#### gRPC connection {#grpc-connection}

* **Component value:** `grpc-conn`.
* **Logged operations:** Connection lifecycle changes (connect/disconnect).
* **How to enable:** Enable the `EnableGrpcAudit` feature flag.

This source does not add component-specific attributes beyond the common attribute set.

#### gRPC login {#grpc-login}

* **Component value:** `grpc-login`.
* **Logged operations:** gRPC authentication.
* **Log classes:** `Login`.
* **Log phases:** `Completed`.

#|
|| Attribute | Description ||
|| `login_user` | User name.</br>Required. ||
|| `login_user_level` | Value recorded for successful administrator logins. It only takes `admin` value.</br>Optional. ||
#|

#### Monitoring service {#monitoring}

* **Component value:** `monitoring`.
* **Logged operations:** HTTP requests handled by the monitoring service.
* **Log classes:** `ClusterAdmin`.
* **Log phases:** `Received`, `Completed`.

#|
|| Attribute | Description ||
|| `method` | HTTP request method.</br>Required. ||
|| `url` | Request path without query parameters.</br>Required. ||
|| `params` | Query string parameters.</br>Optional. ||
|| `body` | Request body (truncated to 2 MB with the `TRUNCATED_BY_YDB` suffix).</br>Optional. ||
#|

#### Heartbeat {#heartbeat}

* **Component value:** `audit`.
* **Logged operations:** Periodic audit heartbeat messages.
* **Log classes:** `AuditHeartbeat`.
* **Log phases:** `Completed`.

#|
|| Attribute | Description ||
|| `node_id` | Node identifier.</br>Required. ||
#|

#### BSC {#bsc}

* **Component value:** `bsc`.
* **Logged operations:** Configuration replacement requests (`TEvControllerReplaceConfigRequest`) emitted by the console.
* **How to enable:** Only basic audit configuration required.

#|
|| Attribute | Description ||
|| `old_config` | Previous configuration snapshot.</br>Optional. ||
|| `new_config` | New configuration snapshot.</br>Optional. ||
#|

#### Distconf {#distconf}

* **Component value:** `distconf`.
* **Logged operations:** Distributed configuration changes.
* **How to enable:** Only basic audit configuration required.

#|
|| Attribute | Description ||
|| `old_config` | Previous configuration snapshot.</br>Required. ||
|| `new_config` | New configuration snapshot.</br>Required. ||
#|

#### Web login {#web-login}

* **Component value:** `web-login`.
* **Logged operations:** Web console logout events.
* **How to enable:** Only basic audit configuration required.

This source does not add component-specific attributes beyond the common set.

#### Console {#console}

* **Component value:** `console`.
* **Logged operations:** Dynamic configuration changes and tenant database lifecycle events.
* **How to enable:** Only basic audit configuration required.

#|
|| Attribute | Description ||
|| `old_config` | Previous configuration snapshot.</br>Optional. ||
|| `new_config` | New configuration snapshot.</br>Optional. ||
#|

> **Note:** YMQ-specific audit sources and the YDBCP deployment variant use a different audit logging configuration and are described in dedicated internal guides.

## Enabling audit log {#enabling-audit-log}

Delivering events to the audit log stream is enabled for the entire {{ ydb-short-name }} cluster. To enable it, add, to the [cluster configuration](../reference/configuration/index.md), the `audit_config` section, and specify in it one of the stream destinations (`file_backend`, `unified_agent_backend`, `stderr_backend`) or their combination:
Delivering events to the audit log stream is enabled for the entire {{ ydb-short-name }} cluster. Add the `audit_config` section to the [cluster configuration](../reference/configuration/index.md) and specify the desired destinations and log classes.

```yaml
audit_config:
  file_backend:
    format: audit_log_format
    file_path: "path_to_log_file"
  unified_agent_backend:
    format: audit_log_format
    log_name: session_meta_log_name
  stderr_backend:
    format: audit_log_format
```
### `TAuditConfig` parameters {#audit-config}

The `audit_config` section maps directly to the `TAuditConfig` protobuf message. All fields are optional unless stated otherwise.

#|
|| Field | Description ||
|| `stderr_backend` | Settings for forwarding audit events to `stderr`. See the [`TStderrBackend`](#backend-settings) structure. ||
|| `file_backend` | Settings for writing audit events to a file on each node. See the [`TFileBackend`](#backend-settings) structure. ||
|| `unified_agent_backend` | Settings for streaming audit events to Unified Agent. See the [`TUnifiedAgentBackend`](#backend-settings) structure. ||
|| `log_class_config` | Array of per-log-class overrides. Each entry corresponds to `TLogClassConfig`. ||
|| `heartbeat` | Optional heartbeat configuration (`THeartbeatSettings`). ||
#|

#### Backend settings {#backend-settings}

Each backend supports the following fields:

#|
|| Field | Description ||
|| `format` | Output format. Available values: `JSON` (default), `TXT`, `JSON_LOG_COMPATIBLE`. ||
|| `file_path` | For `file_backend` only. Path to the audit log file. Created automatically if it does not exist. Required for the file backend. ||
|| `log_name` | For `unified_agent_backend` only. Session metadata used by Unified Agent routing rules. ||
|| `log_json_envelope` | JSON template that wraps each log record. The template must contain the `%message%` placeholder, which is replaced with the serialized audit record. ||
#|

#### Log class configuration {#log-class-config}

Each entry in `log_class_config` accepts the following fields:

#|
|| Field | Description ||
|| `file_backend` | Write the audit log to a file at each cluster node.</ul>Optional. ||
|| `format` | Audit log format. The default value is `JSON`.<br/>Acceptable values:<ul><li>`JSON`: Serialized [JSON](https://en.wikipedia.org/wiki/JSON).</li><li>`TXT`: Text format.</ul>Optional. ||
|| `file_path` | Path to the file that the audit log will be streamed to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it.<br/>This parameter is required if you use `file_backend`. ||
|| `unified_agent_backend` | Stream the audit log to the Unified Agent. In addition, you need to define the `uaclient_config` section in the [cluster configuration](../reference/configuration/index.md).</ul>Optional. ||
|| `log_name` | The session metadata delivered with the message. Using the metadata, you can redirect the log stream to one or more child channels based on the condition: `_log_name: "session_meta_log_name"`.<br/>Optional. ||
|| `stderr_backend` | Forward the audit log to the standard error stream (`stderr`).</ul>Optional. ||
|| `log_class_config` | Each entry lets you select a log class, enable or disable logging for |
|| `log_class_config.log_class` | Selects the request class to configure. Uses values from the [log classes](#log-classes) list.<br/>Optional (defaults to `Default`). ||
|| `log_class_config.enable_logging` | Enables audit event emission for the selected log class.<br/>Optional. ||
|| `log_class_config.exclude_account_type` | Array of account type (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`) that should exclude events even if logging is enabled.<br/>Optional. ||
|| `log_class_config.log_phase` | Array of request [processing phases to log](#log-phases).<br/>Optional. ||
|| `unified_agent_backend.log_json_envelope` | Same as `stderr_backend.log_json_envelope`, but applied to the Unified Agent destination.<br/>Optional. ||
|| `heartbeat` | Configures periodic heartbeat events emitted by the `AuditHeartbeat` log class. This key accepts the nested fields listed below.<br/>Optional. ||
|| `heartbeat.interval_seconds` | Interval in seconds between heartbeat records.<br/>Optional. ||



### Log classes {#log-classes}
The supported log classes cover different API surfaces:

|| Log class | Value | Description ||
|| `ClusterAdmin` | `1` | Cluster operations. ||
|| `DatabaseAdmin` | `2` | Database operations. ||
|| `Login` | `3` | Login operations. ||
|| `NodeRegistration` | `4` | Node registration. ||
|| `Ddl` | `5` | Ddl operations. ||
|| `Dml` | `6` | Dml operations. ||
|| `Operations` | `7` | Operations ?. ||
|| `ExportImport` | `8` | Export and import operations. ||
|| `Acl` | `9` | Access control operations. ||
|| `AuditHeartbeat` | `10` | Synthetic heartbeat messages that confirm audit logging remains operational. ||
|| `Default` | `1000` | Default settings for any component that doesn't have a configuration entry. ||

### Log phases {#log-phases}
The log phases are defined as follows:

|| Log phase | Description ||
|| `Received` | Event was received. ||
|| `Completed` | Event processing is complete. ||

#### Heartbeat settings {#heartbeat-settings}

Heartbeat events help you monitor the health of the audit logging subsystem. They let you set alerts for missing audit events without triggering false positives on workloads that legitimately stay idle for extended periods.

`heartbeat.interval_seconds` controls how often audit heartbeat events are written. A value of `0` disables heartbeat messages.


### Envelope format {#envelope}

Backends can wrap audit events into a custom JSON envelope by specifying the `log_json_envelope` field. The template must contain a `%message%` placeholder that is replaced with the serialized audit record in the selected format.

When a JSON envelope is configured, each audit event is substituted into the template before being delivered to the backend. The following configuration produces the static envelope expected by observability pipelines that require a `destination` field and an `event.text_data` payload:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{"audit": %message%}'
```

### Config samples {#config-samples}

Write audit events to a text file:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
  log_class_config:
    - log_class: Default
      enable_logging: true
      log_phase: [Received, Completed]
```

Sample configuration that saves the audit log text to Yandex Unified Agent with the `audit` label and outputs it to `stderr` in JSON format:

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
  log_class_config:
    - log_class: ClusterAdmin
      enable_logging: true
      log_phase: [Received, Completed]
    - log_class: AuditHeartbeat
      enable_logging: true
      log_phase: [Completed]
      exclude_account_type: [Anonymous]
  heartbeat:
    interval_seconds: 60
```

## Examples {#examples}

 Fragment of audit log file in `JSON` format.
Fragment of an audit log file in `JSON` format.

```json
2023-03-13T20:05:19.776132Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"562949953476313","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T19:59:27.614731Z: {"paths":"[/my_dir/db1/some_table]","tx_id":"562949953426315","database":"/my_dir/db1","remote_address":"{none}","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE TABLE","component":"schemeshard"}
2023-03-13T20:10:44.345767Z: {"paths":"[/my_dir/db1/some_table, /my_dir/db1/another_table]","tx_id":"562949953506313","database":"{none}","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"ALTER TABLE RENAME","component":"schemeshard"}
2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
```

Event that occurred at `2023-03-13T20:05:19.776132Z` in JSON-pretty:

```json
{
  "paths": "[/my_dir/db1/some_dir]",
  "tx_id": "562949953476313",
  "database": "/my_dir/db1",
  "remote_address": "ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx",
  "status": "SUCCESS",
  "subject": "{none}",
  "detailed_status": "StatusAccepted",
  "operation": "CREATE DIRECTORY",
  "component": "schemeshard"
}
```

The same events in `TXT` format will look as follows:

```txt
2023-03-13T20:05:19.776132Z: component=schemeshard, tx_id=844424930186969, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
2023-03-13T19:59:27.614731Z: component=schemeshard, tx_id=562949953426315, remote_address={none}, subject={none}, database=/my_dir/db1, operation=CREATE TABLE, paths=[/my_dir/db1/some_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:10:44.345767Z: component=schemeshard, tx_id=562949953506313, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database={none}, operation=ALTER TABLE RENAME, paths=[/my_dir/db1/some_table, /my_dir/db1/another_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
```

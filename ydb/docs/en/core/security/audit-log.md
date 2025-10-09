# Audit log

_An audit log_ is a stream that includes data about all the operations that tried to change the {{ ydb-short-name }} objects, successfully or unsuccessfully:

* Database: Creating, editing, and deleting databases.
* Directory: Creating and deleting.
* Table: Creating or editing table schema, changing the number of partitions, backup and recovery, copying and renaming, and deleting tables.
* Topic: Creating, editing, and deleting.
* ACL: Editing.

The data of the audit log stream can be delivered to:

* File on each {{ ydb-short-name }} cluster node.
* Agent for delivering [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/) metrics.
* Standard error stream, `stderr`.

You can use any of the listed destinations or their combinations.

If you forward the stream to a file, access to the audit log is set by file-system rights. Saving the audit log to a file is recommended for production installations.

Forwarding the audit log to the standard error stream (`stderr`) is recommended for test installations. Further stream processing is determined by the {{ ydb-short-name }} cluster [logging](../devops/observability/logging.md) settings.

## Audit log events {#events}

The information about each operation is saved to the audit log as a separate event. Each event includes a set of attributes. Some attributes are common across events, while other attributes are determined by the specific {{ ydb-short-name }} component that generated the event.

#|
|| Attribute | Description ||
|| **Common attributes** | > ||
|| `subject` | Event source SID (`<login>@<subsystem>` format). Unless mandatory authentication is enabled, the attribute will be set to `{none}`.<br/>Required. ||
|| `sanitized_token` | A partially masked authentication token used to link related events while keeping the original credentials hidden. If authentication was not performed, the value will be `{none}`.<br/>Optional. ||
|| `operation` | Operation name (for example, `ALTER DATABASE`, `CREATE TABLE`).<br/>Required. ||
|| `status` | Operation completion status.<br/>Acceptable values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li><li>`IN-PROCESS`: The operation is in progress.</li></ul>Required. ||
|| `reason` | Error message.<br/>Optional. ||
|| `component` | Name of the {{ ydb-short-name }} component that generated the event. Common values include: `schemeshard`, `grpc-proxy`, `grpc-conn`, `grpc-login`, `web-login`, `console`, `bsc`, `monitoring`, `audit`, `distconf`, `ymq`.<br/>Optional. ||
|| `request_id` | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context.<br/>Optional. ||
|| `remote_address` | The IP of the client that delivered the request.<br/>Optional. ||
|| `detailed_status` | The status delivered by a {{ ydb-short-name }} component (for example, `StatusAccepted`, `StatusInvalidParameter`, `StatusNameConflict`).<br/>Optional. ||
|| `cloud_id` | Cloud identifier of the {{ ydb-short-name }} database.<br/>Optional. ||
|| `folder_id` | Folder identifier of the {{ ydb-short-name }} database.<br/>Optional. ||
|| `resource_id` | Resource identifier of the {{ ydb-short-name }} database.<br/>Optional. ||
|| `database` | Database path (for example, `/my_dir/db`).<br/>Optional. ||
|| **Attributes of the SchemeShard component** | > ||
|| `new_owner` | The SID of the new owner of the object when ownership is transferred. <br/>Optional. ||
|| `acl_add` | List of added permissions in [short notation](./short-access-control-notation.md) (for example, `[+R:someuser]`).<br/>Optional. ||
|| `acl_remove` | List of revoked permissions in [short notation](./short-access-control-notation.md) (for example, `[-R:someuser]`).<br/>Optional. ||
|| `user_attrs_add` | List of custom attributes added when creating objects or updating attributes (for example, `[attr_name1: A, attr_name2: B]`).<br/>Optional. ||
|| `user_attrs_remove` | List of custom attributes removed when creating objects or updating attributes (for example, `[attr_name1, attr_name2]`).<br/>Optional. ||
|| `tx_id` | Unique transaction ID. Similarly to `request_id`, this ID can be used to differentiate events related to different operations.<br/>Required. ||
|| `paths` | List of paths in the database that are changed by the operation (for example, `[/my_dir/db/table-a, /my_dir/db/table-b]`).<br/>Required. ||
|| **Attributes of the gRPC proxy component** | > ||
|| `grpc_method` | Fully qualified RPC method that triggered the request (for example, `Ydb.Table.V1.TableService/AlterTable`).<br/>Optional. ||
|| `start_time` | ISO 8601 timestamp when request processing started.<br/>Required. ||
|| `end_time` | ISO 8601 timestamp describing when request finished.<br/>Required. ||
|| `request` | Textual representation of the RPC payload that was sent to {{ ydb-short-name }}. Large requests may be truncated.<br/>Optional. ||
|| **Attributes of the monitoring service** | > ||
|| `method` | HTTP method of the request (for example, `POST`).<br/>Required. ||
|| `url` | Requested URL path.<br/>Required. ||
|| `params` | Query string parameters that accompanied the HTTP request.<br/>Optional. ||
|| `body` | HTTP request body. Over 2MB - truncated.<br/>Optional. ||
|#

## Enabling audit log {#enabling-audit-log}

Delivering events to the audit log stream is enabled for the entire {{ ydb-short-name }} cluster. To enable it, add, to the [cluster configuration](../reference/configuration/index.md), the `audit_config` section, and specify in it one of the stream destinations (`file_backend`, `unified_agent_backend`, `stderr_backend`) or their combination:

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

| Key | Description |
| --- | --- |
| `file_backend` | Write the audit log to a file at each cluster node.</ul>Optional. |
| `format` | Audit log format. The default value is `JSON`.<br/>Acceptable values:<ul><li>`JSON`: Serialized [JSON](https://en.wikipedia.org/wiki/JSON).</li><li>`TXT`: Text format.</ul>Optional. |
| `file_path` | Path to the file that the audit log will be streamed to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it.<br/>This parameter is required if you use `file_backend`. |
| `unified_agent_backend` | Stream the audit log to the Unified Agent. In addition, you need to define the `uaclient_config` section in the [cluster configuration](../reference/configuration/index.md).</ul>Optional. |
| `log_name` | The session metadata delivered with the message. Using the metadata, you can redirect the log stream to one or more child channels based on the condition: `_log_name: "session_meta_log_name"`.<br/>Optional. |
| `stderr_backend` | Forward the audit log to the standard error stream (`stderr`).</ul>Optional. |
| `log_class_config` | Each entry lets you select a log class, enable or disable logging for |
| `log_class_config.log_class` | Selects the request class to configure. Uses values from the [log classes](#log-classes) list.<br/>Optional (defaults to `Default`). |
| `log_class_config.enable_logging` | Enables audit event emission for the selected log class.<br/>Optional. |
| `log_class_config.exclude_account_type` | Array of account type (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`) that should exclude events even if logging is enabled.<br/>Optional. |
| `log_class_config.log_phase` | Array of request [processing phases to log](#log-phases).<br/>Optional. |
| `unified_agent_backend.log_json_envelope` | Same as `stderr_backend.log_json_envelope`, but applied to the Unified Agent destination.<br/>Optional. |
| `heartbeat` | Configures periodic heartbeat events emitted by the `AuditHeartbeat` log class. This key accepts the nested fields listed below.<br/>Optional. |
| `heartbeat.interval_seconds` | Interval in seconds between heartbeat records.<br/>Optional. |

Heartbeat events help you monitor the health of the audit logging subsystem. They let you set alerts for missing audit events without triggering false positives on workloads that legitimately stay idle for extended periods.

When a JSON envelope is configured, each audit event is substituted into the template before being delivered to the backend. The following configuration produces the static envelope expected by observability pipelines that require a `destination` field and an `event.text_data` payload:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{ "destination": "topicname", "event": { "text_data": "%message%" } }'
```

### Log classes {#log-classes}
The supported log classes cover different API surfaces:

| Log class | Value | Description |
| --- | --- | --- |
| `ClusterAdmin` | `1` | Cluster operations. |
| `DatabaseAdmin` | `2` | Database operations. |
| `Login` | `3` | Login operations. |
| `NodeRegistration` | `4` | Node registration. |
| `Ddl` | `5` | Ddl operations. |
| `Dml` | `6` | Dml operations. |
| `Operations` | `7` | Operations ?. |
| `ExportImport` | `8` | Export and import operations. |
| `Acl` | `9` | Access control operations. |
| `AuditHeartbeat` | `10` | Synthetic heartbeat messages that confirm audit logging remains operational. |
| `Default` | `1000` | Default settings for any component that doesn't have a configuration entry. |

### Log phases {#log-phases}
The log phases are defined as follows:

| Log phase | Description |
| --- | --- |
| `Received` | Event was received. |
| `Completed` | Event processing is complete. |

### Config samples {#config-samples}

Sample configuration that saves the audit log text to `/var/log/ydb-audit.log`:

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

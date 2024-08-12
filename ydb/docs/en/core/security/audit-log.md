# Audit log

_An audit log_ is a stream that includes data about all the operations that tried to change the {{ ydb-short-name }} objects, successfully or unsuccessfully:

* Database: Creating, editing, and deleting databases.
* Directory: Creating and deleting.
* Table: Creating or editing table schema, changing the number of partitions, backup and recovery, copying and renaming, and deleting tables.
* Topic: Creating, editing, and deleting.
* ACL: Editing.

The data of the audit log stream can be delivered to:

* File on each {{ ydb-short-name }} cluster node.
* Agent for delivering [Unified Agent](https://cloud.yandex.ru/docs/monitoring/concepts/data-collection/unified-agent/) metrics.
* Standard error stream, `stderr`.

You can use any of the listed destinations or their combinations.

If you forward the stream to a file, access to the audit log is set by file-system rights. Saving the audit log to a file is recommended for production installations.

Forwarding the audit log to the standard error stream (`stderr`) is recommended for test installations. Further stream processing is determined by the {{ ydb-short-name }} cluster [logging](../devops/manual/logging.md) settings.

## Audit log events {#events}

The information about each operation is saved to the audit log as a separate event. Each event includes a set of attributes. Some attributes are common across events, while other attributes are determined by the specific {{ ydb-short-name }} component that generated the event.

| Attribute | Description |
|:----|:----|
| **Common attributes** |
| `subject` | Event source SID (`<login>@<subsystem>` format). Unless mandatory authentication is enabled, the attribute will be set to `{none}`.<br/>Required. |
| `operation` | Names of operations or actions are similar to the YQL syntax (for example, `ALTER DATABASE`, `CREATE TABLE`).<br/>Required. |
| `status` | Operation completion status.<br/>Acceptable values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li><li>`IN-PROCESS`: The operation is in progress.</li></ul>Required. |
| `reason` | Error message.<br/>Optional. |
| `component` | Name of the {{ ydb-short-name }} component that generated the event (for example, `schemeshard`).<br/>Optional. |
| `request_id` | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context.<br/>Optional. |
| `remote_address` | The IP of the client that delivered the request.<br/>Optional. |
| `detailed_status` | The status delivered by a {{ ydb-short-name }} component (for example, `StatusAccepted`, `StatusInvalidParameter `, `StatusNameConflict`).<br/>Optional. |
| **Ownership and permission attributes** |
| `new_owner` | The SID of the new owner of the object when ownership is transferred. <br/>Optional. |
| `acl_add` | List of added permissions in [short notation](./short-access-control-notation.md) (for example, `[+R:someuser]`).<br/>Optional. |
| `acl_remove` | List of revoked permissions in [short notation](./short-access-control-notation.md) (for example, `[-R:someuser]`).<br/>Optional. |
| **Custom attributes** |
| `user_attrs_add` | List of custom attributes added when creating objects or updating attributes (for example, `[attr_name1: A, attr_name2: B]`).<br/>Optional. |
| `user_attrs_remove` | List of custom attributes removed when creating objects or updating attributes (for example, `[attr_name1, attr_name2]`).<br/>Optional. |
| **Attributes of the SchemeShard component** |
| `tx_id` | Unique transaction ID. Similarly to `request_id`, this ID can be used to differentiate events related to different operations.<br/>Required. |
| `database` | Database path (for example, `/my_dir/db`).<br/>Required. |
| `paths` | List of paths in the database that are changed by the operation (for example, `[/my_dir/db/table-a, /my_dir/db/table-b]`).<br/>Required. |

## Enabling audit log {#enabling-audit-log}

Delivering events to the audit log stream is enabled for the entire {{ ydb-short-name }} cluster. To enable it, add, to the [cluster configuration](../deploy/configuration/config.md), the `audit_config` section, and specify in it one of the stream destinations (`file_backend`, `unified_agent_backend`, `stderr_backend`) or their combination:

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
--- | ---
| `file_backend` | Write the audit log to a file at each cluster node.</ul>Optional. |
| `format` | Audit log format. The default value is `JSON`.<br/>Acceptable values:<ul><li>`JSON`: Serialized [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}.</li><li>`TXT`: Text format.</ul>Optional. |
| `file_path` | Path to the file that the audit log will be streamed to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it.<br/>This parameter is required if you use `file_backend`. |
| `unified_agent_backend` | Stream the audit log to the Unified Agent. In addition, you need to define the `uaclient_config` section in the [cluster configuration](../deploy/configuration/config.md).</ul>Optional. |
| `log_name` | The session metadata delivered with the message. Using the metadata, you can redirect the log stream to one or more child channels based on the condition: `_log_name: "session_meta_log_name"`.<br/>Optional. |
| `stderr_backend` | Forward the audit log to the standard error stream (`stderr`).</ul>Optional. |

Sample configuration that saves the audit log text to `/var/log/ydb-audit.log`:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Sample configuration that saves the audit log text to Yandex Unified Agent with the `audit` label and outputs it to `stderr` in JSON format:

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
```

## Examples {#examples}

 Fragment of audit log file in `JSON` format.

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

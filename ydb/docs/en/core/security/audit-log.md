# Audit logging

_Audit log_ is a stream which records user induced activity with objects and data in {{ ydb-short-name }}.

Audit log makes it possible to monitor activities with [schema entities](audit-log-scheme.md), [data in tables](audit-log-dml.md), and track [client connections](audit-log-conn.md). Basically, audit log provides information about what actions were performed on which objects from which ip addresses on behalf of what users or services. As such, audit log helps organizations to create a historical record of activity to comply with regulatory requirements or enforce other policies.

## Audit log records and attributes

Information about actions or operations is persisted as audit log records. Each record consists of a set of fields or attributes. Some attributes are the same for all records, and others are determined by the type of activity or the type of operation described.

| __Attribute__ | __Description__ |
|:----|:----|
| **Common** |
| `component` | Name of the {{ ydb-short-name }} component that generated the event (for example, `schemeshard`, `grpc-conn`, `grpc-proxy`). |
| `subject` | User SID (`<login>@<subsystem>` format). Unless mandatory authentication is enabled, the attribute will be set to `{none}`.<br>Required. |
| `operation` | Names of operations or actions are similar to the YQL syntax (for example, `ALTER DATABASE`, `CREATE TABLE`).<br>Required. |
| `database` | Database path (for example, `/root/db`). |
| `status` | General operation status.<br>Possible values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li></ul>Required. |
| `detailed_status` | Detailed operation status. Component specific. |
| `reason` | Error message. |
| `request_id` | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context. |
| `remote_address` | The IP of the client that delivered the request. |
| `start_time` | Operation start time. |
| `end_time` | Operation end time. |
| **Schema operations** | (`schemeshard` component)
| `tx_id` | Unique transaction ID. Similarly to `request_id`, this ID can be used to differentiate events related to different operations.<br>Required. |
| `paths` | List of paths in the database that are changed by the operation (for example, `[/root/db/table-a, /root/db/table-b]`).<br>Required. |
| _Ownership and permissions_ ||
| `new_owner` | User SID of the new owner of the object when ownership is transferred.  |
| `acl_add` | List of added permissions, in [short notation](./short-access-control-notation.md) (for example, `[+R:someuser]`). |
| `acl_remove` | List of revoked permissions, in [short notation](./short-access-control-notation.md) (for example, `[-R:somegroup]`). |
| _[User (or custom) attributes](../dev/custom-attributes.md)_ |
| `user_attrs_add` | List of custom attributes added when creating objects or updating attributes (for example, `[attr_name1: A, attr_name2: B]`). |
| `user_attrs_remove` | List of custom attributes removed when creating objects or updating attributes (for example, `[attr_name1, attr_name2]`). |
| **[Credentials](../concepts/auth#static-credentials) management operations** | (`schemeshard` component)
| `login_user` | User name (when [adding or modifying the user](./access-management.md#users)).
| `login_group` | Group name (when [adding or modifying the user group](./access-management.md#groups)).
| `login_member` | User name (when [adding or removing the user to or from a group](./access-management.md#groups)).
| **DML operations** | (`grpc-proxy` component)
| `query_text` | YQL query text.
| `prepared_query_id` | Prepared query ID.
| `tx_id` | Unique transaction ID.
| `begin_tx` | Request for implicit transaction start.
| `commit_tx` | Request for implicit transaction commit.
| `table` | Path of the affected table.
| `row_count` | Number of the changed rows.

## Usage and configuration

Audit logging subsystem supports recording the stream to:

* a file on each {{ ydb-short-name }} cluster node;
* standard error stream `stderr`;
* [Unified Agent](https://cloud.yandex.com/en/docs/monitoring/concepts/data-collection/unified-agent/) &mdash; agent for delivering metrics and logs.

It is possible to use any of the destinations listed above, as well as their combination.

Writing to a file or unified-agent is recommended for use in production installations. In the case of writing to a file, access to the audit log is set by file-system rights. {{ ydb-short-name }} does not handle logfile compression or rotation.

Writing to `stderr` is recommended for test installations. Further processing of `stderr` may be affected by the settings of the [logging](../devops/manual/logging.md) subsystem.

### Settings

The audit logging require configuration at the {{ ydb-short-name }} cluster level.

It should be configured by adding the `audit_config` section with a set of destination backends to the [cluster configuration](../deploy/configuration/config.md):

```yaml
audit_config:
  file_backend:
    format: <OUTPUT_FORMAT>
    file_path: "path_to_file"
  unified_agent_backend:
    format: <OUTPUT_FORMAT>
    log_name: session_meta_log_name
  stderr_backend:
    format: <OUTPUT_FORMAT>
```

Each backend (`file_backend`, `unified_agent_backend`, `stderr_backend`) is optional, but at least one should be present. Thus, the `audit_config` section should not be empty.

| Key | Description |
--- | ---
| `*.format` | Audit log format. <br>Supported values:<ul><li>`JSON` — serialized [JSON](https://{{ lang }}.wikipedia.org/wiki/JSON).</li><li>`TXT` — plain text (comma-separated `<field>=<value>` pairs, see example below).</ul>_Optional_.<br>Default value: `JSON`.
| `stderr_backend` | `stderr` backend.
| `file_backend` | Local file backend.
| `file_backend.file_path` | Path to a file. Any missing path component will be created on each node at node startup. If the file exists, the data will be appended to it.<br>_Required for `file_backend`_.
| `unified_agent_backend` | Unified Agent backend.<br>Unified Agent client should be configured by `uaclient_config` section in the [cluster configuration](../deploy/configuration/config.md).
| `unified_agent_backend.log_name` | A field in the Unified Agent session metadata that will be passed along with the messages. Allows log stream dispatch to one or more child channels according to the condition `_log_name: "session_meta_log_name"`.<br>_Optional_.<br>Default value: value of `uaclient_config.log_name`.

### How to enable {#enabling-audit-log}

Audit logging is disabled by default.

Adding `audit_config` section to the [cluster configuration](../deploy/configuration/config.md) is enough to enable audit logging.

Additional requirements for enabling logging of individual activities are described in the relevant sections: [Client connections](./audit-log-conn.md#how-to-enable), [Schema operations](./audit-log-scheme.md#how-to-enable), [DML operations](./audit-log-dml.md#how-to-enable).

Example configuration for writing audit log to the file `/var/log/ydb-audit.log` in `TXT` format:

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Example configuration for writing audit log to Unified Agent with the `audit` label in `TXT` format and to `stderr` in `JSON` format:

```yaml
audit_config:
  unified_agent_backend:
    format: TXT
    log_name: audit
  stderr_backend:
    format: JSON
```

## Audit log examples {#examples}

In `JSON` format:

```json
2023-03-13T20:05:19.776132Z: {"paths":"[/root/db1/some_dir]","tx_id":"562949953476313","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/root/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/root/db1/some_dir]","tx_id":"844424930216970","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
2023-03-13T19:59:27.614731Z: {"paths":"[/root/db1/some_table]","tx_id":"562949953426315","database":"/root/db1","remote_address":"{none}","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"CREATE TABLE","component":"schemeshard"}
2023-03-13T20:10:44.345767Z: {"paths":"[/root/db1/some_table, /root/db1/another_table]","tx_id":"562949953506313","database":"{none}","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"ALTER TABLE RENAME","component":"schemeshard"}
2023-03-14T10:41:36.485788Z: {"paths":"[/root/db1/some_dir]","tx_id":"281474976775658","database":"/root/db1","remote_address":"xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
```

In `TXT` format:

```txt
2023-03-13T20:05:19.776132Z: component=schemeshard, tx_id=844424930186969, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=CREATE DIRECTORY, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=CREATE DIRECTORY, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/root/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
2023-03-13T19:59:27.614731Z: component=schemeshard, tx_id=562949953426315, remote_address={none}, subject={none}, database=/root/db1, operation=CREATE TABLE, paths=[/root/db1/some_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-13T20:10:44.345767Z: component=schemeshard, tx_id=562949953506313, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database={none}, operation=ALTER TABLE RENAME, paths=[/root/db1/some_table, /root/db1/another_table], status=SUCCESS, detailed_status=StatusAccepted
2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx, subject={none}, database=/root/db1, operation=MODIFY ACL, paths=[/root/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
```

# Audit log

_An audit log_ is a stream of records that document the operation of the {{ ydb-short-name }} cluster. Unlike technical logs, which help detect failures and troubleshoot issues, the audit log provides data relevant to security. It serves as a source of information that answers the questions: who did what, when, and from where.

A single audit log record may look like this:

```json
{"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","operation":"ExecuteQueryRequest","database":"/my_dir/db1","status":"SUCCESS","subject":"serviceaccount@as","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]"}
```

Examples of typical audit log events:

* Data access through DML requests.
* Schema or configuration management operations.
* Changes to permissions or access-control settings.
* Administrative user actions.

The [`audit_config`](#audit-log-configuration) section in the cluster configuration defines which audit logs are collected, how they need to be serialized and where they are delivered.

## Key concepts {#audit-log-concepts}

### Audit events {#audit-events}

An *audit event* is a record in the audit log that captures a single security-relevant action. Every event includes attributes that describe different aspects of the event. The common attributes are listed in the [Common attributes](#common-attributes) section.

### Audit event sources {#audit-event-sources}

An *audit event source* is a {{ ydb-short-name }} service or subsystem that can emit audit events. Each source is identified by a unique identifier (UID) and may expose additional attributes specific to the component. Some sources require extra configuration, such as feature flags, before the source starts emitting events. See the [Audit event sources overview](#audit-event-sources-overview) for details.

### Log classes {#log-classes}

Audit events are grouped into *log classes* that represent broad categories of operations. You can enable or disable logging for each class in [configuration](#log-class-config) and, if necessary, tailor the configuration per class. The available log classes are:

#|
|| Log class.         | Description ||
|| `ClusterAdmin`     | Cluster administration requests. ||
|| `DatabaseAdmin`    | Database administration requests. ||
|| `Login`            | Login requests. ||
|| `NodeRegistration` | Node registration. ||
|| `Ddl`              | DDL requests. ||
|| `Dml`              | DML requests. ||
|| `Operations`       | Asynchronous remote procedure call (RPC) operations that require polling to track the result. ||
|| `ExportImport`     | Export and import data operations. ||
|| `Acl`              | Access Control List (ACL) operations. ||
|| `AuditHeartbeat`   | Synthetic heartbeat messages that confirm audit logging remains operational. ||
|| `Default`          | Default settings for any component that doesn't have a configuration entry. ||
|#

At the moment, not all audit event sources categorize events into log classes. For most of them, the [basic configuration](#enabling-audit-log) is sufficient to capture their events. See the [Audit event sources overview](#audit-event-sources-overview) section for details.

### Log phases {#log-phases}

Some audit event sources divide the request processing into stages. *Logging phase* indicates the processing stages at which audit logging records events. Specifying logging phases is useful when you need fine-grained visibility into request execution and want to capture events before and after critical processing steps. The available log phases are:

#|
|| Log phase      | Description ||
|| `Received`     | A request is received and the initial checks and authentication are made. The `status` attribute is set to `IN-PROCESS`. </br>This phase is disabled by default; you must include `Received` in `log_class_config.log_phase` to enable it. ||
|| `Completed`    | A request is completely finished. The `status` attribute is set to `SUCCESS` or `ERROR`. This phase is enabled by default when `log_class_config.log_phase` is not set. ||
|#

### Audit log destinations {#stream-destinations}

*Audit log destination* is a target where the audit log stream can be delivered.

You can currently configure the following destinations for the audit log:

* A file on each {{ ydb-short-name }} cluster node.
* An agent for delivering [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/) metrics.
* The standard error stream, `stderr`.

You can use any of the listed destinations or their combinations. See the [audit log configuration](#audit-log-configuration) for details.

If you forward the stream to a file, file-system permissions control access to the audit log. Saving the audit log to a file is recommended for production installations.

For test installations, forward the audit log to the standard error stream (`stderr`). Further stream processing depends on the {{ ydb-short-name }} cluster [logging](../devops/observability/logging.md) settings.

## Audit event sources overview {#audit-event-sources-overview}

The table below summarizes the built-in audit event sources. Use it to identify which source emits the events you need and how to enable those events.

#|
|| Source / UID | What it records | Configuration requirements ||
|| [Schemeshard](#schemeshard) </br>`schemeshard` | Schema operations, ACL modifications, and user management actions. | Included in the [basic audit configuration](#enabling-audit-log). ||
|| [gRPC services](#grpc-proxy) </br>`grpc-proxy` | Non-internal requests handled by {{ ydb-short-name }} gRPC endpoints. | Enable the relevant [log classes](#log-class-config) and optional [log phases](#log-phases). ||
|| [gRPC connection](#grpc-connection) </br>`grpc-conn` | Client connection and disconnection events. | Enable the [`enable_grpc_audit`](../reference/configuration/feature_flags.md) feature flag. ||
|| [gRPC authentication](#grpc-login) </br>`grpc-login` | gRPC authentication attempts. | Enable the `Login` class in [`log_class_config`](#log-class-config). ||
|| [Monitoring service](#monitoring) </br>`monitoring` | HTTP requests handled by the monitoring endpoint. | Enable the `ClusterAdmin` class in [`log_class_config`](#log-class-config). ||
|| [Heartbeat](#heartbeat) </br>`audit` | Synthetic heartbeat events proving that audit logging is alive. | Enable the `AuditHeartbeat` class in [`log_class_config`](#log-class-config) and optionally adjust [heartbeat settings](#heartbeat-settings). ||
|| [BlobStorage Controller](#bsc) </br>`bsc` | Console-driven BlobStorage Controller configuration changes. | Included in the [basic audit configuration](#enabling-audit-log). ||
|| [Distconf](#distconf) </br>`distconf` | Distributed configuration updates. | Included in the [basic audit configuration](#enabling-audit-log). ||
|| [Web login](#web-login) </br>`web-login` | Interactions with the web console authentication widget. | Included in the [basic audit configuration](#enabling-audit-log). ||
|| [Console](#console) </br>`console` | Database lifecycle operations and dynamic configuration changes. | Included in the [basic audit configuration](#enabling-audit-log). ||
|#

## Audit event attributes {#audit-event-attributes}

As mentioned, attributes are divided into two groups:
* Common attributes present in many *audit event sources* and always carry the same meaning.
* Attributes specific to the source that generates the event.

In this section, you will find a reference guide to the attributes in audit events. It covers both common attributes and source-specific ones. For each source, its UID, recorded operations, and configuration requirements are also provided.

### Common attributes {#common-attributes}

The table below lists the common attributes.

#|
|| Attribute          | Description ||
|| `subject`          | Event source SID (`<login>@<subsystem>` format). Unless mandatory authentication is enabled, the attribute will be set to `{none}`. ||
|| `sanitized_token`  | A partially masked authentication token that was used to execute the request. Can be used to link related events while keeping the original credentials hidden. If authentication was not performed, the value will be `{none}`. ||
|| `operation`        | Operation name (for example, `ALTER DATABASE`, `CREATE TABLE`). ||
|| `component`        | Unique identifier (UID) of the *audit event source*. ||
|| `status`           | Operation completion status.<br/>Acceptable values:<ul><li>`SUCCESS`: The operation completed successfully.</li><li>`ERROR`: The operation failed.</li><li>`IN-PROCESS`: The operation is in progress.</li></ul> ||
|| `reason`           | Error message. ||
|| `request_id`       | Unique ID of the request that invoked the operation. You can use the `request_id` to differentiate events related to different operations and link the events together to build a single audit-related operation context. ||
|| `remote_address`   | IP of the client that delivered the request. ||
|| `detailed_status`  | The status delivered by a {{ ydb-short-name }} *audit event source*. ||
|| `database`         | Database path (for example, `/my_dir/db`). ||
|| `cloud_id`         | Cloud identifier of the {{ ydb-short-name }} database. ||
|| `folder_id`        | Folder identifier of the {{ ydb-short-name }} cluster or database. ||
|| `resource_id`      | Resource identifier of the {{ ydb-short-name }} database.<br/>*Optional.* ||
|#

### Schemeshard {#schemeshard}

* **UID:** `schemeshard`.
* **Logged operations:** Schema operations triggered by DDL queries, ACL modifications, and user management operations.
* **How to enable:** Only [basic audit configuration](#enabling-audit-log) required.

The table below lists additional attributes specific to the `Schemeshard` source.

#|
|| Attribute                                | Description ||
|| **Common schemeshard attributes**        | **>** ||
|| `tx_id`                                  | Unique transaction ID. This ID can be used to differentiate events related to different operations.</br>*Required.* ||
|| `paths`                                  | List of paths in the database that are changed by the operation (for example, `[/my_dir/db/table-a, /my_dir/db/table-b]`).</br>*Optional.* ||
|| **Ownership and permission attributes**  | **>** ||
|| `new_owner`                              | SID of the new owner of the object when ownership is transferred. ||
|| `acl_add`                                | List of added permissions in [short notation](./short-access-control-notation.md) (for example, `[+R:someuser]`). ||
|| `acl_remove`                             | List of revoked permissions in [short notation](./short-access-control-notation.md) (for example, `[-R:someuser]`). ||
|| **Custom attributes**                    | **>** ||
|| `user_attrs_add`                         | List of custom attributes added when creating objects or updating attributes (for example, `[attr_name1: A, attr_name2: B]`). ||
|| `user_attrs_remove`                      | List of custom attributes removed when creating objects or updating attributes (for example, `[attr_name1, attr_name2]`). ||
|| **Login/Auth specific**                  | **>** ||
|| `login_user`                             | User name recorded by login operations. ||
|| `login_group`                            | Group name recorded by login operations. ||
|| `login_member`                           | Membership changes. ||
|| `login_user_change`                      | Changes applied to user settings. ||
|| `login_user_level`                       | Privilege level of the user recorded by audit events. This attribute only uses the `admin` value. ||
|| **Import/Export operation attributes**   | **>** ||
|| `id`                                     | Unique identifier for export or import operations. ||
|| `uid`                                    | User-defined label for operations. ||
|| `start_time`                             | Operation start time in ISO 8601 format. ||
|| `end_time`                               | Operation end time in ISO 8601 format. ||
|| `last_login`                             | User's last successful login time in ISO 8601 format. ||
|| **Export-specific**                      | **>** ||
|| `export_type`                            | Export destination. Possible values: `yt`, `s3`. ||
|| `export_item_count`                      | Number of exported items. ||
|| `export_yt_prefix`                       | YT destination path prefix. ||
|| `export_s3_bucket`                       | S3 bucket used for exports. ||
|| `export_s3_prefix`                       | S3 destination prefix. ||
|| **Import-specific**                      | **>** ||
|| `import_type`                            | Import source type. It's always `s3`. ||
|| `import_item_count`                      | Number of imported items. ||
|| `import_s3_bucket`                       | S3 bucket used for imports. ||
|| `import_s3_prefix`                       | S3 source prefix. ||
|#

### gRPC services {#grpc-proxy}

* **UID:** `grpc-proxy`.
* **Logged operations:** All non-internal gRPC requests.
* **How to enable:** Requires specifying log classes in audit configuration.
* **Log classes:** Depends on the RPC request type: `Ddl`, `Dml`, `Operations`, `ClusterAdmin`, `DatabaseAdmin`, or other classes.
* **Log phases:** `Received`, `Completed`.

Тhe table below lists additional attributes specific to the `gRPC services` source.

#|
|| Attribute                  | Description ||
|| **Common gRPC attributes** | **>** ||
|| `grpc_method`              | RPC method name.</br>*Optional.* ||
|| `request`                  | Sanitized representation of the incoming request.</br>*Optional.* ||
|| `start_time`               | Operation start time in ISO 8601 format.</br>*Required.* ||
|| `end_time`                 | Operation end time in ISO 8601 format.</br>*Optional.* ||
|| **Transaction attributes** | **>** ||
|| `tx_id`                    | Transaction identifier. ||
|| `begin_tx`                 | Flag set to `1` when the request starts a new transaction. ||
|| `commit_tx`                | Shows whether the request commits the transaction. Possible values: `true`, `false`. ||
|| **Request fields**         | **>** ||
|| `query_text`               | Sanitized [YQL](../core/yql/reference/index.md) query text. ||
|| `prepared_query_id`        | Identifier of a prepared query. ||
|| `program_text`             | [MiniKQL program](../concepts/glossary.md#minikql) sent with the request. ||
|| `schema_changes`           | Description of schema modifications requested in the operation. ||
|| `table`                    | Full table path. ||
|| `row_count`                | Number of rows processed by a [bulk upsert](../recipes/ydb-sdk/bulk-upsert.md) request. ||
|| `tablet_id`                | Tablet identifier. ||
|#

### gRPC connection {#grpc-connection}

* **UID:** `grpc-conn`.
* **Logged operations:** Connection state changes (connect/disconnect).
* **How to enable:** Enable the `enable_grpc_audit` [feature flag](../reference/configuration/feature_flags.md).

*This source uses only common attributes.*

### gRPC authentication {#grpc-login}

* **UID:** `grpc-login`.
* **Logged operations:** gRPC authentication.
* **How to enable:** Requires specifying log classes in [audit configuration](#audit-log-configuration).
* **Log classes:** `Login`.
* **Log phases:** `Completed`.

The table below lists additional attributes specific to the `gRPC authentication` source.

#|
|| Attribute          | Description ||
|| `login_user`       | User name. *Required.* ||
|| `login_user_level` | Privilege level of the user recorded by audit events. This attribute only uses the `admin` value. *Optional.* ||
|#

### Monitoring service {#monitoring}

* **UID:** `monitoring`.
* **Logged operations:** HTTP requests handled by the monitoring service.
* **How to enable:** Requires specifying log classes in [audit configuration](#audit-log-configuration).
* **Log classes:** `ClusterAdmin`.
* **Log phases:** `Received`, `Completed`.

The table below lists additional attributes specific to the `Monitoring service` source.

#|
|| Attribute  | Description ||
|| `method`   | HTTP request method. For example `POST`, `GET`.</br>*Required.* ||
|| `url`      | Request path without query parameters.</br>*Required.* ||
|| `params`   | Raw query parameters.</br>*Optional.* ||
|| `body`     | Request body (truncated to 2 MB with the `TRUNCATED_BY_YDB` suffix).</br>*Optional.* ||
|#

### Heartbeat {#heartbeat}

* **UID:** `audit`.
* **Logged operations:** Periodic audit [heartbeat](#heartbeat-settings) messages.
* **How to enable:** Enable this source by specifying log classes in [audit configuration](#audit-log-configuration).
* **Log classes:** `AuditHeartbeat`.
* **Log phases:** `Completed`.

The table below lists additional attributes specific to the `Heartbeat` source.

#|
|| Attribute | Description ||
|| `node_id` | Node identifier where the event occurred. *Required.* ||
|#

### BlobStorage Controller {#bsc}

* **UID:** `bsc`.
* **Logged operations:** Configuration replacement requests (`TEvControllerReplaceConfigRequest`) emitted by the console.
* **How to enable:** Only [basic audit configuration](#enabling-audit-log) required.

The table below lists additional attributes specific to the `BlobStorage Controller` source.

#|
|| Attribute    | Description ||
|| `old_config` | Snapshot of the previous BlobStorage Controller configuration in YAML form. </br>*Optional.* ||
|| `new_config` | Snapshot of the configuration that replaced the previous one. </br>*Optional.* ||
|#

### Distconf {#distconf}

* **UID:** `distconf`.
* **Logged operations:** Distributed configuration changes.
* **How to enable:** Only [basic audit configuration](#enabling-audit-log) required.

The table below lists additional attributes specific to the `Distconf` source.

#|
|| Attribute    | Description ||
|| `old_config` | Snapshot of the configuration that was active before the distributed update was accepted. Distconf serializes it in YAML. *Required.* ||
|| `new_config` | Snapshot of the configuration that Distconf committed after the change. *Required.* ||
|#

### Web login {#web-login}

* **UID:** `web-login`.
* **Logged operations:** Tracks interactions with the {{ ydb-short-name }} web console authentication widget.
* **How to enable:** Only [basic audit configuration](#enabling-audit-log) required.

*This source uses only common attributes.*

### Console {#console}

* **UID:** `console`.
* **Logged operations:** Database lifecycle operations and dynamic configuration changes.
* **How to enable:** Only [basic audit configuration](#enabling-audit-log) required.

The table below lists additional attributes specific to the `Console` source.

#|
|| Attribute    | Description ||
|| `old_config` | Snapshot of the configuration that was in effect before the console request was applied. *Optional.* ||
|| `new_config` | Snapshot of the configuration that the console applied. *Optional.* ||
|#

## Audit log configuration {#audit-log-configuration}

### Enabling audit log {#enabling-audit-log}

Audit logging works cluster-wide. For the *basic configuration*, add the `audit_config` section to the [cluster configuration](../reference/configuration/index.md) and specify one or more stream destinations (`file_backend`, `unified_agent_backend`, `stderr_backend`):

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

### Audit config parameters {#audit-config}

All fields are optional.

#|
#| Field                    | Description ||
|| `stderr_backend`         | Forward the audit log to the standard error stream (`stderr`). See the [backend settings](#backend-settings) for details. ||
|| `file_backend`           | Write the audit log to a file at each cluster node. See the [backend settings](#backend-settings) for details. ||
|| `unified_agent_backend`  | Stream the audit log to the [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/). In addition, you need to define the `uaclient_config` section in the [cluster configuration](../reference/configuration/index.md). See the [backend settings](#backend-settings) for details. ||
|| `log_class_config`       | An array of audit rules for different log classes. See the [log class configuration](#log-class-config). ||
|| `heartbeat`              | Optional heartbeat configuration. See the [heartbeat settings](#heartbeat-settings). ||
|#

### Backend settings {#backend-settings}

Each backend supports the following fields:

#|
|| Field                | Description ||
|| `format`             | Audit log format. The default value is `JSON`. See [Log format](#log-format) for details.<br/>*Optional.* ||
|| `file_path`          | Path to the file that the audit log will be streamed to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it. Only for `file_backend`. <br/>*Required.* ||
|| `log_name`           | The session metadata delivered with the message. Using the metadata, you can redirect the log stream to one or more child channels based on the condition: `_log_name: "session_meta_log_name"`. Only for `unified_agent_backend`. <br/>*Optional.* ||
|| `log_json_envelope`  | JSON template that wraps each log record. The template must contain the `%message%` placeholder, which is replaced with the serialized audit record. See the [Envelope format](#envelope-format).</br>*Optional.* ||
|#

#### Log format {#log-format}

The `format` field specifies the serialization format for audit events. The supported formats are:

#|
|| Format                 | Description ||
|| `JSON`                 | Each audit event is serialized as a single-line JSON object preceded by an ISO 8601 timestamp.</br>Example: `<time>: {"k1": "v1", "k2": "v2", ...}` </br>*`k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|| `TXT`                  | Each audit event is serialized as a single-line text string in the `key=value` format, preceded by an ISO 8601 timestamp.</br>Example: `<time>: k1=v1, k2=v2, ...` </br>*`k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|| `JSON_LOG_COMPATIBLE`  | Each audit event is serialized as a single-line JSON object suitable for output to destinations shared with debug logs. The object contains the `@timestamp` field with the ISO 8601 timestamp and the `@log_type` field set to `audit`.</br>Example: `{"@timestamp": "<ISO 8601 time>", "@log_type": "audit", "k1": "v1", "k2": "v2", ...}` </br>*`@timestamp` stores the ISO 8601 timestamp; `k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|#

#### Envelope format {#envelope-format}

Backends can wrap audit events into a custom envelope before delivering them to the backend by specifying the `log_json_envelope` field. The template must contain the `%message%` placeholder, which is replaced with the serialized audit record in the selected format.

For example, the following configuration outputs audit events to `stderr` in JSON format, wrapped in a custom envelope:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{"audit": %message%, "source": "ydb-audit-log"}'
```

See the [Example with Envelope JSON](#examples) section for output details.

### Log class configuration {#log-class-config}

Each entry in `log_class_config` accepts the following fields:

#|
|| Field                  | Description ||
|| `log_class`            | Class name to configure. Uses values from the [log classes](#log-classes) list. The `log_class_config` list must not contain two classes with the same name.<br/>*Required.* ||
|| `enable_logging`       | Enables audit event emission for the selected log class. Disabled by default.<br/>*Optional.* ||
|| `exclude_account_type` | Array of account type (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`) that should exclude events even if logging is enabled.<br/>*Optional.* ||
|| `log_phase`            | Array of request processing phases to log. See the [Log phases](#log-phases).<br/>*Optional.* ||
|#

### Heartbeat settings {#heartbeat-settings}

Heartbeat events help you monitor the health of the audit logging subsystem. They allow you to create alerts for missing audit events without false positives during periods of no activity.

`heartbeat.interval_seconds` controls how often audit heartbeat events are written. A value of `0` disables heartbeat messages.

### Config samples {#config-samples}

Below is a simple configuration that saves the audit log text to a file in `TXT` format.

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

Sample configuration that sends the audit log to Unified Agent in `TXT` format with the `audit` label and also outputs it to `stderr` in `JSON` format. The `Default` settings enable logging for all classes in the `Completed` phase. Additionally, `ClusterAdmin` is configured to log the `Received` phase, and `DatabaseAdmin` is configured to exclude events from anonymous users:

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
    - log_class: DatabaseAdmin
      enable_logging: true
      log_phase: [Completed]
      exclude_account_type: [Anonymous]
    - log_class: Default
      enable_logging: true
  heartbeat:
    interval_seconds: 60
```

## Examples {#examples}

The following tabs show the same audit log event written using different [backend settings](#backend-settings).

{% list tabs %}

- JSON

    The `JSON` format produces entries like:

    ```json
    2023-03-14T10:41:36.485788Z: {"paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}", "detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    2023-03-13T20:07:30.927210Z: {"reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","sanitized_token":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    2025-11-03T18:07:39.056211Z: {"@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","sanitized_token":"xxxxxxxx.**","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as"}
    2025-11-03T17:41:44.203214Z: {"component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- TXT

    The `TXT` format produces entries like:

    ```txt
    2023-03-14T10:41:36.485788Z: component=schemeshard, tx_id=281474976775658, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=MODIFY ACL, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusSuccess, acl_add=[+(ConnDB):subject:-]
    2023-03-13T20:07:30.927210Z: component=schemeshard, tx_id=281474976775657, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject={none}, database=/my_dir/db1, operation=CREATE DIRECTORY, paths=[/my_dir/db1/some_dir], status=SUCCESS, detailed_status=StatusAlreadyExists, reason=Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)
    2025-11-03T18:07:39.056211Z: component=grpc-proxy, tx_id=281474976775656, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx, subject=serviceaccount@as, database=/my_dir/db1, operation=ExecuteQueryRequest, query_text=SELECT * FROM `my_row_table`; status=SUCCESS, detailed_status=StatusSuccess, begin_tx=1, commit_tx=1, end_time=2025-11-03T18:07:39.056204Z, grpc_method=Ydb.Query.V1.QueryService/ExecuteQuery, sanitized_token=xxxxxxxx.**, start_time=2025-11-03T18:07:39.054863Z
    2025-11-03T17:41:44.203214Z: component=monitoring, remote_address=ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx], operation=HTTP REQUEST, method=POST, url=/viewer/query, params=base64=false&schema=multipart, body={"query":"SELECT * FROM `my_row_table`;","database":"/local","action":"execute-query","syntax":"yql_v1"}, status=IN-PROCESS, reason=Execute
    ```

- JSON_LOG_COMPATIBLE

    The `JSON_LOG_COMPATIBLE` format produces entries like:

    ```json
    {"@timestamp":"2023-03-14T10:41:36.485788Z","@log_type":"audit","paths":"[/my_dir/db1/some_dir]","tx_id":"281474976775658","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAccepted","operation":"MODIFY ACL","component":"schemeshard","acl_add":"[+(ConnDB):subject:-]"}
    {"@timestamp":"2023-03-13T20:07:30.927210Z","@log_type":"audit","reason":"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)","paths":"[/my_dir/db1/some_dir]","tx_id":"844424930216970","database":"/my_dir/db1","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx","status":"SUCCESS","subject":"{none}","detailed_status":"StatusAlreadyExists","operation":"CREATE DIRECTORY","component":"schemeshard"}
    {"@timestamp":"2025-11-03T18:07:39.056211Z","@log_type":"audit","begin_tx":1,"commit_tx":1,"component":"grpc-proxy","database":"/my_dir/db1","detailed_status":"SUCCESS","end_time":"2025-11-03T18:07:39.056204Z","grpc_method":"Ydb.Query.V1.QueryService/ExecuteQuery","operation":"ExecuteQueryRequest","query_text":"SELECT * FROM `my_row_table`;","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","start_time":"2025-11-03T18:07:39.054863Z","status":"SUCCESS","subject":"serviceaccount@as","sanitized_token":"xxxxxxxx.**"}
    {"@timestamp":"2025-11-03T17:41:44.203214Z","@log_type":"audit","component":"monitoring","remote_address":"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]","operation":"HTTP REQUEST","method":"POST","url":"/viewer/query","params":"base64=false&schema=multipart","body":"{\"query\":\"SELECT * FROM `my_row_table`;\",\"database\":\"/local\",\"action\":\"execute-query\",\"syntax\":\"yql_v1\"}","status":"IN-PROCESS","reason":"Execute"}
    ```

- Envelope JSON

    The JSON envelope template `{"message": %message%, "source": "ydb-audit-log"}` produces entries like:

    ```json
    {"message":"2023-03-14T10:41:36.485788Z: {\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"281474976775658\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAccepted\",\"operation\":\"MODIFY ACL\",\"component\":\"schemeshard\",\"acl_add\":\"[+(ConnDB):subject:-]\"}\n","source":"ydb-audit-log"}
    {"message":"2023-03-13T20:07:30.927210Z: {\"reason\":\"Check failed: path: '/my_dir/db1/some_dir', error: path exist, request accepts it (id: [OwnerId: 72075186224037889, LocalPathId: 3], type: EPathTypeDir, state: EPathStateNoChanges)\",\"paths\":\"[/my_dir/db1/some_dir]\",\"tx_id\":\"844424930216970\",\"database\":\"/my_dir/db1\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx\",\"status\":\"SUCCESS\",\"subject\":\"{none}\",\"detailed_status\":\"StatusAlreadyExists\",\"operation\":\"CREATE DIRECTORY\",\"component\":\"schemeshard\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T18:07:39.056211Z: {\"@log_type\":\"audit\",\"begin_tx\":1,\"commit_tx\":1,\"component\":\"grpc-proxy\",\"database\":\"/my_dir/db1\",\"detailed_status\":\"SUCCESS\",\"end_time\":\"2025-11-03T18:07:39.056204Z\",\"grpc_method\":\"Ydb.Query.V1.QueryService/ExecuteQuery\",\"operation\":\"ExecuteQueryRequest\",\"query_text\":\"SELECT * FROM `my_row_table`;\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"sanitized_token\":\"xxxxxxxx.**\",\"start_time\":\"2025-11-03T18:07:39.054863Z\",\"status\":\"SUCCESS\",\"subject\":\"serviceaccount@as\"}\n","source":"ydb-audit-log"}
    {"message":"2025-11-03T17:41:44.203214Z: {\"component\":\"monitoring\",\"remote_address\":\"ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]\",\"operation\":\"HTTP REQUEST\",\"method\":\"POST\",\"url\":\"/viewer/query\",\"params\":\"base64=false&schema=multipart\",\"body\":\"{\\\"query\\\":\\\"SELECT * FROM `my_row_table`;\\\",\\\"database\\\":\\\"/local\\\",\\\"action\\\":\\\"execute-query\\\",\\\"syntax\\\":\\\"yql_v1\\\"}\",\"status\":\"IN-PROCESS\",\"reason\":\"Execute\"}\n","source":"ydb-audit-log"}
    ```

- Pretty-JSON

    The same audit log entry formatted for readability:

    ```json
    {
      "paths": "[/my_dir/db1/some_dir]",
      "tx_id": "281474976775658",
      "database": "/my_dir/db1",
      "remote_address": "ipv6:[xxxx:xxx:xxx:xxx:x:xxxx:xxx:xxxx]:xxxxx",
      "status": "SUCCESS",
      "subject": "{none}",
      "detailed_status": "StatusAccepted",
      "operation": "MODIFY ACL",
      "component": "schemeshard",
      "acl_add": "[+(ConnDB):subject:-]"
    }
    ```

{% endlist %}

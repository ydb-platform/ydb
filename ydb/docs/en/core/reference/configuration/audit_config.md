# audit_config

{{ ydb-short-name }} provides a built-in audit log that captures security-relevant actions performed in the cluster. You can enable and configure audit logging via the cluster configuration file.

## Audit log configuration {#audit-log-configuration}

Audit logging works cluster-wide. For the basic configuration, add the `audit_config` section to the [cluster configuration](../configuration/index.md) and specify one or more stream destinations (`file_backend`, `unified_agent_backend`, `stderr_backend`):

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
|| **Parameter**                | **Description** ||
|| `stderr_backend`         | Forward the audit log to the standard error stream (`stderr`). See the [backend settings](#backend-settings) for details. ||
|| `file_backend`           | Write the audit log to a file at each cluster node. See the [backend settings](#backend-settings) for details. ||
|| `unified_agent_backend`  | Stream the audit log to the [Unified Agent](https://yandex.cloud/docs/monitoring/concepts/data-collection/unified-agent/). In addition, you need to define the `uaclient_config` section in the [cluster configuration](../configuration/log_config.md). See the [backend settings](#backend-settings) for details. ||
|| `log_class_config`       | An array of audit rules for different log classes. See the [log class configuration](#log-class-config). ||
|| `heartbeat`              | Optional heartbeat configuration. See the [heartbeat settings](#heartbeat-settings). ||
|#

### Backend settings {#backend-settings}

Each backend supports the following fields:

#|
|| **Parameter**            | **Description** ||
|| `format`             | Audit log format. The default value is `JSON`. See [Log format](#log-format) for details.<br/>*Optional.* ||
|| `file_path`          | Path to the file that the audit log will be streamed to. If the path and the file are missing, they will be created on each node at cluster startup. If the file exists, the data will be appended to it. Only for `file_backend`. <br/>*Required.* ||
|| `log_name`           | The session metadata delivered with the message. Using the metadata, you can redirect the log stream to one or more child channels based on the condition: `_log_name: "session_meta_log_name"`. Only for `unified_agent_backend`. <br/>*Optional.* ||
|| `log_json_envelope`  | JSON template that wraps each log record. The template must contain the `%message%` placeholder, which is replaced with the serialized audit record. See the [Envelope format](#envelope-format).<br/>*Optional.* ||
|#

### Log format {#log-format}

The `format` field specifies the serialization format for audit events. The supported formats are:

#|
|| **Parameter**             | **Description** ||
|| `JSON`                 | Each audit event is serialized as a single-line JSON object preceded by an ISO 8601 timestamp.<br/>Example: `<time>: {"k1": "v1", "k2": "v2", ...}` <br/>*`k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|| `TXT`                  | Each audit event is serialized as a single-line text string in the `key=value` format, separated by ", " (no escaping is applied to separator inside values), preceded by an ISO 8601 timestamp.<br/>Example: `<time>: k1=v1, k2=v2, ...` <br/>*`k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|| `JSON_LOG_COMPATIBLE`  | Each audit event is serialized as a single-line JSON object suitable for output to destinations shared with debug logs. The object contains the `@timestamp` field with the ISO 8601 timestamp and the `@log_type` field set to `audit`.<br/>Example: `{"@timestamp": "<ISO 8601 time>", "@log_type": "audit", "k1": "v1", "k2": "v2", ...}` <br/>*`@timestamp` stores the ISO 8601 timestamp; `k1`, `k2`, …, `kn` represent audit log attributes; `v1`, `v2`, …, `vn` represent their values.* ||
|#

### Envelope format {#envelope-format}

Backends can wrap audit events into a custom envelope before delivering them to the backend by specifying the `log_json_envelope` field. The template must contain the `%message%` placeholder, which is replaced with the serialized audit record in the selected format.

For formats like `JSON`, `%message%` will be substituted with a value in the form `<timestamp>: <escaped_json>`, where `<timestamp>` is an ISO 8601 formatted timestamp and `<escaped_json>` is the serialized and escaped JSON representation of the audit record.

For example, the following configuration outputs audit events to `stderr` in JSON format, wrapped in a custom envelope:

```yaml
audit_config:
  stderr_backend:
    format: JSON
    log_json_envelope: '{"audit": %message%, "source": "ydb-audit-log"}'
```

### Log class configuration {#log-class-config}

Each entry in `log_class_config` accepts the following fields:

#|
|| **Parameter**              | **Description** ||
|| `log_class`            | Class name to configure. Uses values from the [log classes](../../security/audit-log.md#log-classes) list. The `log_class_config` list must not contain two classes with the same name.<br/>*Required.* ||
|| `enable_logging`       | Enables audit event emission for the selected log class. Disabled by default.<br/>*Optional.* ||
|| `exclude_account_type` | Array of account type (`Anonymous`, `User`, `Service`, `ServiceImpersonatedFromUser`) that should exclude events even if logging is enabled.<br/>*Optional.* ||
|| `log_phase`            | Array of request processing phases to log. See the [log phases](../../security/audit-log.md#log-phases).<br/>*Optional.* ||
|#

### Heartbeat settings {#heartbeat-settings}

Heartbeat events help you monitor the health of the audit logging subsystem. They allow you to create alerts for missing audit events without false positives during periods of no activity.

#|
|| **Parameter**            | **Description** ||
|| `heartbeat.interval_seconds` | Specifies how often audit heartbeat events are written (in seconds). A value of `0` disables heartbeat messages. Default: `0`. ||
|#

### Config samples {#config-samples}

**Simple configuration.** Below is a simple configuration that saves the audit log text to a file in `TXT` format.

```yaml
audit_config:
  file_backend:
    format: TXT
    file_path: "/var/log/ydb-audit.log"
```

**Advanced configuration.** The following configuration demonstrates more advanced settings:

* It sends the audit log to Unified Agent in `TXT` format with the `audit` label and also outputs it to `stderr` in `JSON` format.
* The `Default` settings enable logging for all classes in the `Completed` phase.
* Additionally, `ClusterAdmin` is configured to log the `Received` phase, and `DatabaseAdmin` is configured to exclude events from anonymous users:

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
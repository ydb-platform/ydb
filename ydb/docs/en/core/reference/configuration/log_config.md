# log_config

The `log_config` section controls how {{ ydb-short-name }} server processes and manages its logs. It allows you to customize logging levels for different components, as well as global log formats and output methods.

{% note info %}

This document describes application and system logging configuration. For security and audit logging, see [{#T}](../../security/audit-log.md).

{% endnote %}

## Overview

Logging is a critical part of the {{ ydb-short-name }} [observability](../observability/index.md) system. The `log_config` section lets you configure various aspects of logging, including:

- Default logging level
- Component-specific logging levels
- Log output format
- Integration with system logs or external logging services

## Log Output Methods

- **To stderr**: by default, {{ ydb-short-name }} sends all logs to stderr.
- **To file**: logs can be written to a file using the `backend_file_name` parameter.
- **To syslog**: when the `sys_log: true` parameter is enabled, logs are redirected to the syslog and stop being output to stderr. Logs are sent using `/dev/log` socket.
- **To Unified Agent**: when configuring the `uaclient_config` section, logs are sent to [Unified Agent](https://yandex.cloud/en/docs/monitoring/concepts/data-collection/unified-agent/) and stop being output to stderr.

When both `sys_log` and `uaclient_config` are enabled simultaneously, logs will be sent to both syslog and Unified Agent. If you need to continue outputting logs to stderr while using other methods, activate `sys_log_to_stderr: true`.

## Configuration Options

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `default_level` | uint32 | 5 (`NOTICE`) | Default logging level for all components. |
| `default_sampling_level` | uint32 | 7 (`DEBUG`) | Default sampling level for all components. |
| `default_sampling_rate` | uint32 | 0 | Default sampling rate for all components. If set to N (where N > 0), approximately 1 out of every N log messages with priority between `default_level` and `default_sampling_level` will be logged. For example, to log every 10th message, set to 10. A value of 0 means that no messages in this range will be logged (they are all dropped). |
| `sys_log` | bool | false | Enable system logging via syslog. |
| `sys_log_to_stderr` | bool | false | Copy logs to stderr in addition to system log. |
| `format` | string | "full" | Log output format. Possible values: "full", "short", "json". |
| `cluster_name` | string | — | Cluster name to include in log records. The `cluster_name` field is added to logs only when using the `json` format or when sending to Unified Agent. In the `full` or `short` formats, this field is not displayed. |
| `allow_drop_entries` | bool | true | Allow dropping log entries if the logging system is overloaded. When enabled, log entries are buffered in memory and written to the output when either 10 messages accumulate or the time specified by `time_threshold_ms` elapses. If the buffer becomes full, lower-priority messages may be dropped to make room for higher-priority ones. |
| `use_local_timestamps` | bool | false | Use local time zone for log timestamps (UTC is used by default). |
| `backend_file_name` | string | — | File name for log output. If specified, logs are written to this file. |
| `sys_log_service` | string | — | Service name for syslog. Corresponds to the tag field in the old syslog [RFC 3164](https://datatracker.ietf.org/doc/html/rfc3164) or the app-name field in the modern [RFC 5424](https://datatracker.ietf.org/doc/html/rfc5424) protocol. |
| `time_threshold_ms` | uint64 | 1000 | If `allow_drop_entries = true`, specifies how often {{ ydb-short-name }} writes buffered log messages to the output, in milliseconds. |
| `ignore_unknown_components` | bool | true | Ignore logging requests from unknown components. |
| `entry` | array | [] | Configuration of logging level and/or sampling for specific {{ ydb-short-name }} components, see [{#T}](#entry-objects) below. |
| `uaclient_config` | object | — | Configuration for the Unified Agent client, see [{#T}](#uaclient-config) below. |

### Entry Objects {#entry-objects}

The `entry` field contains an array of objects with the following structure:

| Parameter | Type | Description |
| --- | --- | --- |
| `component` | string | Component name. See the full list of available components [on GitHub](https://github.com/ydb-platform/ydb/blob/main/ydb/library/services/services.proto#L6). |
| `level` | uint32 | [Log level](#log-levels) for this component. |
| `sampling_level` | uint32 | Sampling level for this component. Works similarly to `default_sampling_level`. |
| `sampling_rate` | uint32 | Sampling rate for this component.  Works similarly to `default_sampling_rate`. |

### UAClientConfig Object {#uaclient-config}

The `uaclient_config` field configures integration with [Unified Agent](https://yandex.cloud/en/docs/monitoring/concepts/data-collection/unified-agent/):

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `uri` | string | — | grpc URI of the Unified Agent server. |
| `shared_secret_key` | string | — | Path to the file with the secret key for client connection authentication. |
| `max_inflight_bytes` | uint64 | 100000000 | Maximum number of bytes in transit when sending data. |
| `grpc_reconnect_delay_ms` | uint64 | — | Delay between reconnection attempts in milliseconds. |
| `grpc_send_delay_ms` | uint64 | — | Delay between send attempts in milliseconds. |
| `grpc_max_message_size` | uint64 | — | Maximum gRPC message size. |
| `client_log_file` | string | — | Log file for the UA client itself. |
| `client_log_priority` | uint32 | — | Logging level for the UA client itself. |
| `log_name` | string | — | Log name that is passed in session metadata. |

## Log Levels {#log-levels}

{{ ydb-short-name }} uses the following log levels, listed from the highest to the lowest severity:

| Level | Numeric value | Description |
| --- | --- | --- |
| `EMERG` | 0 | System outage (for example, cluster failure) is possible. |
| `ALERT` | 1 | System degradation is possible, system components may fail. |
| `CRIT` | 2 | A critical state. |
| `ERROR` | 3 | A non-critical error. |
| `WARN` | 4 | A warning, it should be responded to and fixed unless it's temporary. |
| `NOTICE` | 5 | An event essential for the system or the user has occurred. |
| `INFO` | 6 | Debugging information for collecting statistics. |
| `DEBUG` | 7 | Debugging information for developers. |
| `TRACE` | 8 | Detailed debugging information. |

## Examples

### Basic Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  format: "full"
```

This configuration outputs logs to stderr with logging level `NOTICE` and above.

### File Output Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  format: "full"
  backend_file_name: "/var/log/ydb/ydb.log"
```

This configuration sends logs to a file while maintaining the default logging level of `NOTICE`.

### Syslog Output Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  sys_log: true
  sys_log_service: "ydb"
  format: "full"
```

This configuration sends logs to syslog with the service name "ydb".

### Setting Per-Component Log Levels

```yaml
log_config:
  default_level: 5  # NOTICE
  entry:
    - component: "SCHEMESHARD"
      level: 7  # DEBUG
    - component: "TABLET_MAIN"
      level: 6  # INFO
  backend_file_name: "/var/log/ydb/ydb.log"
```

### Sampling Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  default_sampling_level: 7  # DEBUG
  default_sampling_rate: 10  # Log every 10th message between NOTICE and DEBUG
  entry:
    - component: "BLOBSTORAGE"
      sampling_level: 8  # TRACE
      sampling_rate: 100  # Log every 100th message between NOTICE and TRACE
```

This configuration sets up sampling for logs. With default settings, every 10th message with priority between `NOTICE` and `DEBUG` will be logged. For the `BLOBSTORAGE` component, every 100th message with priority between `NOTICE` and `TRACE` will be logged.

### JSON Format Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  format: "json"
  cluster_name: "production-cluster"
  uaclient_config:
    uri: "[fd53::1]:16400"
    grpc_max_message_size: 4194304
    log_name: "ydb_logs"
```

This configuration outputs logs in JSON format and sends them to Unified Agent.

## Notes

- Log levels are specified in the configuration as numeric values, not strings. Use the [table above](#log-levels) to map between numeric values and their meanings.
- If the `backend_file_name` parameter is specified, logs are written to this file. If the `sys_log` parameter is true, logs are sent to the system logger.
- The `format` parameter determines how log entries are formatted. The "full" format includes all available information, "short" provides a more compact format, and "json" outputs logs in JSON format, which is convenient for parsing by logging services.
- The internal log buffer has the following size limits:
  - Default total size: 10MB (10 &ast; 1024 &ast; 1024 bytes)
  - Default grain size: 64KB (1024 &ast; 64 bytes)
  - Maximum message size: 1KB (1024 bytes)

## See Also

- [{#T}](../observability/index.md)
- [{#T}](../observability/metrics/index.md)
- [{#T}](../observability/tracing/setup.md)
- [{#T}](../../security/audit-log.md)

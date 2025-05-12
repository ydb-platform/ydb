# `log_config` configuration section

The `log_config` section controls how {{ ydb-short-name }} server processes and manages its logs. It allows you to customize logging levels, formats, and destinations for different components.

## Overview

Logging is a critical part of {{ ydb-short-name }}'s observability. The `log_config` section lets you configure various aspects of logging, including:

- Default logging level
- Component-specific logging levels
- Log output format
- Log rotation and storage
- Integration with system logs or external logging services

## Configuration Options

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `default_level` | uint32 | 5 (NOTICE) | Default logging level for all components. |
| `default_sampling_level` | uint32 | 7 (DEBUG) | Default sampling level for all components. |
| `default_sampling_rate` | uint32 | 0 | Default sampling rate for all components. |
| `sys_log` | bool | false | Enable system logging via syslog. |
| `sys_log_to_stderr` | bool | false | Copy logs to stderr in addition to system log. |
| `format` | string | "full" | Log output format. Possible values: "full", "short", "json". |
| `cluster_name` | string | — | Cluster name to include in log records. |
| `allow_drop_entries` | bool | true | Allow dropping log entries if the logging system is overloaded. |
| `use_local_timestamps` | bool | false | Use local time zone for log timestamps (UTC is used by default). |
| `backend_file_name` | string | — | File name for log output. If specified, logs are written to this file. |
| `sys_log_service` | string | — | Service name for syslog. |
| `time_threshold_ms` | uint64 | 1000 | Time threshold for log operations in milliseconds. |
| `ignore_unknown_components` | bool | true | Ignore logging requests from unknown components. |
| `tenant_name` | string | — | Database name to include in log records. |
| `entry` | array | [] | Array of component-specific logging configurations. |
| `uaclient_config` | object | — | Configuration for the Unified Agent client. |

### Entry Objects

The `entry` field contains an array of objects with the following structure:

| Parameter | Type | Description |
| --- | --- | --- |
| `component` | string | Component name (must be base64-encoded when specified in YAML). |
| `level` | uint32 | Log level for this component. |
| `sampling_level` | uint32 | Sampling level for this component. |
| `sampling_rate` | uint32 | Sampling rate for this component. |

### UAClientConfig Object

The `uaclient_config` field configures integration with [Unified Agent](https://yandex.cloud/en/docs/monitoring/concepts/data-collection/unified-agent/):

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `uri` | string | — | URI of the Unified Agent server. |
| `shared_secret_key` | string | — | Shared secret key for authentication. |
| `max_inflight_bytes` | uint64 | 100000000 | Maximum number of bytes in transit. |
| `grpc_reconnect_delay_ms` | uint64 | — | Delay between reconnection attempts in milliseconds. |
| `grpc_send_delay_ms` | uint64 | — | Delay between send attempts in milliseconds. |
| `grpc_max_message_size` | uint64 | — | Maximum gRPC message size. |
| `client_log_file` | string | — | Log file for the UA client itself. |
| `client_log_priority` | uint32 | — | Log priority for the UA client. |
| `log_name` | string | — | Log name for session metadata. |

## Log Levels {#log-levels}

{{ ydb-short-name }} uses the following log levels, listed from the highest to the lowest severity:

| Level | Numeric value | Description |
| --- | --- | --- |
| EMERG | 0 | System outage (for example, cluster failure) is possible. |
| ALERT | 1 | System degradation is possible, system components may fail. |
| CRIT | 2 | A critical state. |
| ERROR | 3 | A non-critical error. |
| WARN | 4 | A warning, it should be responded to and fixed unless it's temporary. |
| NOTICE | 5 | An event essential for the system or the user has occurred. |
| INFO | 6 | Debugging information for collecting statistics. |
| DEBUG | 7 | Debugging information for developers. |
| TRACE | 8 | Detailed debugging information. |

## Examples

### Basic Configuration

```yaml
log_config:
  default_level: 5  # NOTICE
  sys_log: true
  format: "full"
  backend_file_name: "/var/log/ydb/ydb.log"
```

### Setting Per-Component Log Levels

```yaml
log_config:
  default_level: 5  # NOTICE
  entry:
    - component: U0NIRU1FU0hBUkQ=  # Base64 for "SCHEMESHARD"
      level: 7  # DEBUG
    - component: VEFCTEVUX01BSU4=  # Base64 for "TABLET_MAIN"
      level: 6  # INFO
  backend_file_name: "/var/log/ydb/ydb.log"
```

### JSON Format with Unified Agent Integration

```yaml
log_config:
  default_level: 5  # NOTICE
  format: "json"
  cluster_name: "production-cluster"
  sys_log: false
  uaclient_config:
    uri: "[fd53::1]:16400"
    grpc_max_message_size: 4194304
    log_name: "ydb_logs"
```

### Full Example

```yaml
log_config:
  default_level: 5  # NOTICE
  default_sampling_level: 7  # DEBUG
  default_sampling_rate: 10
  sys_log: true
  sys_log_to_stderr: true
  format: "json"
  cluster_name: "production-cluster"
  allow_drop_entries: true
  use_local_timestamps: false
  backend_file_name: "/var/log/ydb/ydb.log"
  sys_log_service: "ydb"
  time_threshold_ms: 2000
  ignore_unknown_components: true
  tenant_name: "main"
  entry:
    - component: U0NIRU1FU0hBUkQ=  # Base64 for "SCHEMESHARD"
      level: 7  # DEBUG
    - component: VEFCTEVUX01BSU4=  # Base64 for "TABLET_MAIN"
      level: 6  # INFO
    - component: QkxPQlNUT1JBR0U=  # Base64 for "BLOBSTORAGE"
      level: 4  # WARN
  uaclient_config:
    uri: "[fd53::1]:16400"
    grpc_max_message_size: 4194304
    log_name: "ydb_logs"
```

## Notes

- When specifying component names in the `entry` array, the component names must be base64-encoded.
- Log levels are specified as numeric values rather than strings in the configuration. Use [the table above](#log-levels) to determine the numeric values of log levels.
- If `backend_file_name` is specified, logs are written to that file. If `sys_log` is set to true, logs are sent to the system logger.
- The `format` parameter determines how log entries are formatted. The "full" format includes all available information, "short" provides a more compact format, and "json" outputs logs in JSON format for easier parsing.
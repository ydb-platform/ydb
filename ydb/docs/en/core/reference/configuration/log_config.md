# `log_config` configuration section

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

## Configuration Options

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `default_level` | uint32 | 5 (NOTICE) | Default logging level for all components. |
| `default_sampling_level` | uint32 | 7 (DEBUG) | Default sampling level for all components. |
| `default_sampling_rate` | uint32 | 0 | Default sampling rate for all components. If set to N (where N > 0), approximately 1 out of every N log messages with priority between `default_level` and `default_sampling_level` will be logged. For example, to log every 10th message, set to 10. A value of 0 means that no messages in this range will be logged (they are all dropped). |
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
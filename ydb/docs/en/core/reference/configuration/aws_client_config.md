# aws_client_config

The `aws_client_config` section defines defaults for the AWS SDK client that {{ ydb-short-name }} uses when accessing S3 and other object storage systems with a compatible API. It configures timeouts, TLS settings, and logging for all such outbound connections made by the cluster node process; a specific storage configuration can override these values when needed.

## Syntax

```yaml
aws_client_config:
  log_config:
    log_level: 0
    filename_prefix: ""
  verify_ssl: true
  request_timeout_ms: 0
  http_request_timeout_ms: 0
  connection_timeout_ms: 10000
  enable_tcp_keep_alive: true
  tcp_keep_alive_interval_ms: 30000
  max_connections_count: 32
  executor_threads_count: 32
  ca_path: "/etc/ssl/certs"
```

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `log_config.log_level` | `0` (Off) | Numeric AWS SDK log verbosity level. See [Log levels](#aws-sdk-log-levels) for level names and numeric values. |
| `log_config.filename_prefix` | — | Prefix for AWS SDK log files. When set, files are created as `<prefix>YYYY-MM-DD-HH.log`; otherwise logs go to stdout. |
| `verify_ssl` | `true` | Whether to validate TLS certificates when connecting to AWS endpoints and compatible APIs. Set to `false` only in test environments. |
| `request_timeout_ms` | `0` (disabled) | Request timeout at the AWS SDK level for the entire request, including DNS lookup, connection setup, TLS handshake, and data transfer. The value is specified in milliseconds. `0` means no SDK-side timeout limit. |
| `http_request_timeout_ms` | `0` (disabled) | HTTP-layer timeout in milliseconds applied by the HTTP client (Curl). `0` means no timeout. |
| `connection_timeout_ms` | `10000` | TCP connect timeout in milliseconds. |
| `enable_tcp_keep_alive` | `true` | Enables TCP keep-alive on client sockets. |
| `tcp_keep_alive_interval_ms` | `30000` (AWS SDK default) | Interval between TCP keep-alive probes in milliseconds. When not set in the YDB config, the AWS SDK default of 30000 ms is used. In Curl this value is rounded to whole seconds and must be at least 15000. |
| `max_connections_count` | `32` | Maximum number of concurrent TCP connections that the HTTP client inside AWS SDK may open. |
| `executor_threads_count` | `32` | Size of the thread pool allocated per unique S3 endpoint. Controls parallelism for asynchronous requests to that endpoint. |
| `ca_path` | `"/etc/ssl/certs"` | Path to a directory with CA certificates used to verify TLS for outbound connections. |
| `ca_file` | — | Path to a single CA certificate bundle file. Used when the trust store is not in the default location. |

### Log levels {#aws-sdk-log-levels}

| Level | Value | Description |
| --- | --- | --- |
| `Off` | `0` | Disable AWS SDK logging. |
| `Fatal` | `1` | Fatal errors only. |
| `Error` | `2` | Non-fatal errors. |
| `Warn` | `3` | Warnings that may need attention. |
| `Info` | `4` | High-level informational messages. |
| `Debug` | `5` | Debug information. |
| `Trace` | `6` | Most verbose diagnostic output. |

## See also

- [{#T}](log_config.md) for logging of the {{ ydb-short-name }} server process. The `log_config` block nested under `aws_client_config` applies only to AWS SDK and is unrelated to the server logging configuration.
- [{#T}](../ydb-cli/export-import/index.md) for S3 import and export from the CLI. Storage access is configured there, not in `aws_client_config`.

## Related scenarios ("S3 in YDB")

Other object storage tasks. Settings from these materials may override the values configured in the `aws_client_config` section.

- [{#T}](../../yql/reference/syntax/create-external-data-source.md) for an external data source with the S3 protocol in YQL.
- [{#T}](../../devops/deployment-options/manual/federated-queries/index.md) for federated queries and external data sources.
- [{#T}](../../recipes/import-export-column-tables.md) for importing and exporting column-oriented tables via object storage.

# aws_client_config

The `aws_client_config` section defines default settings for AWS SDK clients that {{ ydb-short-name }} uses for S3 and other AWS-compatible storage interactions. Values here are applied whenever a per-storage configuration does not override them.

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
  ca_file: ""
```

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `log_config.log_level` | `0` (Off) | AWS SDK log level (`Off`, `Fatal`, `Error`, `Warn`, `Info`, `Debug`, `Trace` mapped to `0–6`). Controls verbosity for AWS client logs. |
| `log_config.filename_prefix` | — | Prefix for AWS SDK log files. When set, files are created as `<prefix>YYYY-MM-DD-HH.log`; otherwise logs go to stdout/stderr. |
| `verify_ssl` | `true` | Whether to validate TLS certificates when connecting to AWS endpoints. Set to `false` only for testing. |
| `request_timeout_ms` | `0` (disabled) | Overall request timeout in milliseconds, including DNS lookup, connect, TLS handshake, and data transfer. `0` keeps the AWS SDK default of no request-level timeout. |
| `http_request_timeout_ms` | `0` (disabled) | HTTP-layer timeout in milliseconds applied by the HTTP client (Curl). `0` keeps the AWS SDK default of no HTTP-layer timeout. |
| `connection_timeout_ms` | `10000` | TCP connect timeout in milliseconds. |
| `enable_tcp_keep_alive` | `true` | Enables TCP keep-alive on client sockets. |
| `tcp_keep_alive_interval_ms` | `30000` | Interval between TCP keep-alive probes in milliseconds. For Curl this is rounded to whole seconds; must be ≥ 15000 ms. |
| `max_connections_count` | `32` | Maximum concurrent TCP connections the AWS HTTP client may open. |
| `executor_threads_count` | `32` | Size of the pooled thread executor shared by AWS clients for a given endpoint. Controls parallelism for async requests. |
| `ca_path` | `"/etc/ssl/certs"` | Path to a directory with CA certificates used to verify TLS connections. |
| `ca_file` | — | Path to a single CA certificate bundle file. Used when the trust store is not in the default location. |

### Log levels

| Level | Value | Description |
| --- | --- | --- |
| `Off` | `0` | Disable AWS SDK logging. |
| `Fatal` | `1` | Fatal errors only. |
| `Error` | `2` | Non-fatal errors. |
| `Warn` | `3` | Warnings that may need attention. |
| `Info` | `4` | High-level informational messages. |
| `Debug` | `5` | Debug information. |
| `Trace` | `6` | Most verbose diagnostic output. |

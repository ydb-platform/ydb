# Logging in {{ ydb-short-name }}

Each {{ ydb-short-name }} component writes messages to logs at different levels. They can be used to detect severe issues or identify the root causes.

## Logging setup {#log_setup}

You can configure logging for the various components of the {{ ydb-short-name }} in the [Embedded UI](../../reference/embedded-ui/logs.md#change_log_level).

There are currently two options for running {{ ydb-short-name }} logging: manually or using systemd.

### Manually {#log_setup_manually}

{{ ydb-short-name }} provides standard mechanisms for collecting logs and metrics.
Logging is done to standard `stdout` and `stderr` streams and can be redirected using popular solutions.

### Using systemd {#log_setup_systemd}

Default logs are written to `journald` and can be retrieved via `journalctl -u ydbd`.

# Logs
Each YDB component writes messages to logs at different levels. They can be used to detect severe issues or identify the root causes of issues.

## Logging setup {#log_setup}
You can configure logging for the various components of the YDB [monitoring system](../maintenance/embedded_monitoring/logs.md#change_log_level).

There are currently two options for running YDB logging.

### Manually {#log_setup_manually}
YDB provides standard mechanisms for collecting logs and metrics.
Logging is done to standard `stdout` and `stderr` streams and can be redirected using popular solutions. We recommend using a combination of Fluentd and Elastic Stack.

### Using systemd {#log_setup_systemd}
Default logs are written to `journald` and can be retrieved via
```
journalctl -u ydbd
```

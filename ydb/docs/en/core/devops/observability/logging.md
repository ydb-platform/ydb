# Logging in {{ ydb-short-name }}

Each {{ ydb-short-name }} component writes messages of different levels to logs. These can be used to detect critical problems or understand the causes of issues.

## Logging Setup {#log_setup}

Logging configuration for individual components can be done in the [embedded interface](../../reference/embedded-ui/logs.md#change_log_level) of {{ ydb-short-name }}.

Currently, there are two options for starting {{ ydb-short-name }} logging: manually and using systemd.

### Manually {#log_setup_manually}

For convenience, {{ ydb-short-name }} provides standard mechanisms for collecting logs and metrics.
Logging is performed to standard `stdout` and `stderr` channels and can be redirected using popular solutions.

### Using Systemd {#log_setup_systemd}

By default, logs are written to `journald` and can be retrieved using `journalctl -u ydbd-storage`. For database nodes, change the systemd unit name appropriately.
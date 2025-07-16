# Cluster Configuration Management Commands

Cluster configuration management commands are designed for working with the configuration at the level of the entire YDB cluster. These commands allow cluster administrators to view, modify, and manage [settings](../../../../../reference/configuration/index.md) that apply to all cluster nodes.

{% include [danger-warning](../_includes/danger-warning.md) %}

General syntax for calling cluster configuration management commands:

```bash
ydb [global options] admin cluster config [command options] <subcommand>
```

Where:

- `ydb` – The command to run the YDB CLI from the operating system command line.
- `[global options]` – Global options, common to all YDB CLI commands.
- `admin cluster config` – The command for managing cluster configuration.
- `[command options]` – Command options specific to each command and subcommand.
- `<subcommand>` – The subcommand.

## Commands {#list}

The following is a list of available subcommands for managing cluster configuration. Any command can be called from the command line with the `--help` option to get help for it.

Command / Subcommand | Brief Description
--- | ---
[admin cluster config fetch](./fetch.md) | Fetch the current dynamic configuration (aliases: `get`, `dump`)
[admin cluster config generate](./generate.md) | Generate dynamic configuration from the static startup configuration
[admin cluster config replace](./replace.md) | Replace the dynamic configuration
admin cluster config vesion | Show configuration version on nodes (V1/V2)

# Bridge mode cluster management commands

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

The [bridge mode](../../../../concepts/bridge.md) cluster management commands let you view the state of each [pile](../../../../concepts/glossary.md#pile), perform planned and emergency PRIMARY switchovers, temporarily take a pile out for maintenance, and then return it back to the cluster.

{% include [danger-warning](../_includes/danger-warning.md) %}

General syntax for bridge mode cluster management commands:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge [command options...] <subcommand>
```

where:

- `{{ ydb-cli }}` — the command to run the {{ ydb-short-name }} CLI from your operating system command line;
- `[global options]` — global options shared by all {{ ydb-short-name }} CLI commands;
- `admin cluster bridge` — the cluster configuration management command;
- `[command options]` — command-specific options for each command and subcommand;
- `<subcommand>` — a subcommand.

## Commands {#list}

Below is a list of available subcommands for managing a cluster in bridge mode. You can run any command with the `--help` option to get help.

Command / subcommand | Brief description
--- | ---
[admin cluster bridge list](./list.md) | Show pile status
[admin cluster bridge switchover](./switchover.md) | Planned `PRIMARY` switchover
[admin cluster bridge failover](./failover.md) | Emergency switchover
[admin cluster bridge takedown](./takedown.md) | Take a pile out of the cluster
[admin cluster bridge rejoin](./rejoin.md) | Return a pile to the cluster

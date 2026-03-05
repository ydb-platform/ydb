# Bridge cluster management commands

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Bridge cluster management commands allow you to view the state of [pile](../../../../concepts/glossary.md#pile), perform planned and emergency PRIMARY changes, temporarily take pile out for maintenance, and return it to the cluster in [bridge mode](../../../../concepts/bridge.md).

{% include [danger-warning](../configuration/_includes/danger-warning.md) %}

General syntax for calling bridge cluster management commands:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge [command options...] <subcommand>
```

where:

- `{{ ydb-cli }}` — the command to run {{ ydb-short-name }} CLI from the operating system command line;
- `[global options]` — global parameters common to all {{ ydb-short-name }} CLI commands;
- `admin cluster bridge` — the cluster configuration management command;
- `[command options]` — command options specific to each command and subcommand;
- `<subcommand>` — the subcommand.

## Commands {#list}

Below is a list of available subcommands for bridge cluster management. Any command can be run with the `--help` option to get help.

Command / subcommand | Brief description
--- | ---
[admin cluster bridge list](./list.md) | Output pile state
[admin cluster bridge switchover](./switchover.md) | Planned `PRIMARY` change
[admin cluster bridge failover](./failover.md) | Emergency switchover
[admin cluster bridge takedown](./takedown.md) | Taking pile out of the cluster
[admin cluster bridge rejoin](./rejoin.md) | Returning pile to the cluster

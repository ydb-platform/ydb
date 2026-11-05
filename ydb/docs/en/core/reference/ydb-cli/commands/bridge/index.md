# Bridge cluster management commands

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Commands for managing the cluster in [bridge mode](../../../../concepts/bridge.md) let you view [pile](../../../../concepts/glossary.md#pile) state, perform planned and emergency PRIMARY change, temporarily take a pile out for maintenance, and return it to the cluster.

{% include [danger-warning](../_includes/danger-warning.md) %}

General syntax for bridge cluster management commands:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge [command options...] <subcommand>
```

where:

- `{{ ydb-cli }}` — command to launch {{ ydb-short-name }} CLI from the operating system command line;
- `[global options]` — global parameters common to all {{ ydb-short-name }} CLI commands;
- `admin cluster bridge` — cluster configuration management command;
- `[command options]` — parameters specific to each command and subcommand;
- `<subcommand>` — subcommand.

## Commands {#list}

Below is the list of available subcommands for bridge cluster management. You can run any command with the `--help` option for help.

Command / subcommand | Brief description
--- | ---
[admin cluster bridge list](./list.md) | List pile state
[admin cluster bridge switchover](./switchover.md) | Planned PRIMARY change
[admin cluster bridge failover](./failover.md) | Emergency failover
[admin cluster bridge takedown](./takedown.md) | Take pile out of cluster
[admin cluster bridge rejoin](./rejoin.md) | Return pile to cluster

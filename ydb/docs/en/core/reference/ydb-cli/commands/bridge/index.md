# Bridge cluster management commands

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Commands for managing the cluster in [bridge mode](../../../../concepts/bridge.md) let you convert an ordinary cluster, view [pile](../../../../concepts/glossary.md#pile) state, add and remove piles, switch database storage modes, and perform planned or emergency pile operations.

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
[admin cluster bridge convert](./convert.md) | Convert an ordinary cluster to a single-pile bridge
[admin cluster bridge list](./list.md) | List pile state and operation progress
[admin cluster bridge provision-pile](./provision-pile.md) | Prepare storage for a new pile
[admin cluster bridge join-pile (rejoin)](./rejoin.md) | Join a provisioned pile or return it after maintenance
[admin cluster bridge switchover](./switchover.md) | Planned PRIMARY change
[admin cluster bridge failover](./failover.md) | Emergency failover
[admin cluster bridge takedown](./takedown.md) | Take pile out of cluster
[admin cluster bridge destroy-pile](./destroy-pile.md) | Delete a disconnected pile's storage and data
[admin cluster bridge set-database-mode](./set-database-mode.md) | Switch a database between bridged and ordinary storage

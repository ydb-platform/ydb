# Configuration Management

The YDB CLI provides commands for managing the [dynamic configuration](../../../../maintenance/manual/dynamic-config.md) at different levels of the system.

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin [scope] config [subcommands...]
```

* `global options` — [Global parameters](../global-options.md).
* `scope` — Configuration scope (`cluster`, `node`).
* `subcommands` — Subcommands for managing configuration.

View the command description:

```bash
{{ ydb-cli }} admin --help
```

## Available Configuration Scopes {#scopes}

### Cluster Configuration {#cluster}

Managing cluster-level configuration:

```bash
{{ ydb-cli }} admin cluster config [subcommands...]
```

Available subcommands:

* [fetch](cluster/fetch.md) - Fetches the current dynamic cluster configuration.
* [generate](cluster/generate.md) - Generates dynamic configuration based on the static configuration on the cluster.
* [replace](cluster/replace.md) - Replaces the dynamic configuration.
* version - Show configuration version on nodes (V1/V2).

### Node Configuration {#node}

Managing node-level configuration:

```bash
{{ ydb-cli }} admin node config [subcommands...]
```

Available subcommands:

* [init](node/init.md) - Initializes the directory for node configuration.

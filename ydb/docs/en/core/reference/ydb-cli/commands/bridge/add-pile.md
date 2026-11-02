# admin cluster bridge add-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge add-pile` command to allocate storage resources for a pile already listed in `bridge_config`. The command creates the pile's parts of bridged storage groups and leaves the pile `DISCONNECTED` so it can then be returned with [rejoin](rejoin.md).

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge add-pile [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge add-pile --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--pile <pile>` | Name of the `INACTIVE` pile to add. |

## Requirements {#requirements}

- Cluster conversion must be complete.
- The pile and its hosts must be present in the configuration, its static nodes must be running, and [list](list.md) must show it as `INACTIVE`.
- `system_tablets` node lists must include nodes from the new pile so system tablets can start there after failover.
- No other pile addition or removal can be pending.
- The pile must have enough drives and failure domains for static groups, State Storage, and all bridged pools.

For the full procedure, see [Listing and adding a pile](../../../../devops/deployment-options/manual/bridge-management.md#add-pile).

## Examples {#examples}

Add the registered pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge add-pile --pile pile-b
```

The command starts a pending `ADD` operation. If there is insufficient space, it fails without changing the configuration. A transient BSC error is retained in the pending operation and retried. Immediately after hosts are registered, `group allocation failed` can also mean that BSC has not discovered them yet; retry after a few seconds.

### Verifying the result {#verify}

Check the operation and pile state:

```bash
{{ ydb-cli }} admin cluster bridge list
```

Wait for the pending `ADD` operation to clear and for the pile to become `DISCONNECTED`, then return it:

```bash
{{ ydb-cli }} -y admin cluster bridge rejoin --pile pile-b
```

The pile synchronizes in the background and becomes `SYNCHRONIZED`. A stuck `ADD` operation can be cancelled with [remove-pile](remove-pile.md).

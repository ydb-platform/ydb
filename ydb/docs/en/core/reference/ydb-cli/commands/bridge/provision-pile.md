# admin cluster bridge provision-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge provision-pile` command to prepare storage for a pile already listed in `bridge_config`. The command creates the pile's parts of static and bridged storage groups and leaves the pile `DISCONNECTED` so it can then be connected with [join-pile](rejoin.md). Ordinary storage groups are not changed.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge provision-pile [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge provision-pile --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--pile <pile>` | Name of the `INACTIVE` pile to provision. |

## Requirements {#requirements}

- Cluster conversion must be complete.
- The pile and its hosts must be present in the configuration, its static nodes must be running, and [list](list.md) must show it as `INACTIVE`.
- The pile must have enough drives and failure domains for static groups, State Storage, and all bridged pools.
- No other pile addition or removal can be in progress.

For the full procedure, see [Adding a pile](../../../../devops/deployment-options/manual/bridge-management.md#add-pile).

## Examples {#examples}

Provision the registered pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge provision-pile --pile pile-b
```

### Verifying the result {#verify}

The command starts a pending `ADD` operation. Use [list](list.md) to wait until this operation clears and the pile becomes `DISCONNECTED`, then join it:

```bash
{{ ydb-cli }} -y admin cluster bridge join-pile --pile pile-b
```

The pile synchronizes in the background and becomes `SYNCHRONIZED`.

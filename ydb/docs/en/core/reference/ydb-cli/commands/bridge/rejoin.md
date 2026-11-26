# admin cluster bridge join-pile (rejoin)

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge join-pile` command to connect a provisioned pile or [return](../../../../concepts/bridge.md#rejoin) a pile to the cluster after maintenance or recovery. `rejoin` is an alias of `join-pile`. The pile transitions from `DISCONNECTED` to `NOT_SYNCHRONIZED`, synchronizes in the background, and then automatically becomes `SYNCHRONIZED`.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge join-pile [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge join-pile --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--pile <pile>` | Name of the pile to join or return to the cluster. |

## Requirements {#requirements}

- The pile must be `DISCONNECTED`. A new pile reaches this state after [provision-pile](provision-pile.md) completes.

## Examples {#examples}

Join provisioned pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge join-pile --pile pile-b
```

Return pile `pile-a` after maintenance using the alias:

```bash
{{ ydb-cli }} -y admin cluster bridge rejoin --pile pile-a
```

### Verifying the result {#verify}

Right after the command runs, the pile is expected to transition to `NOT_SYNCHRONIZED`. Verify with the [list](list.md) command. After synchronization completes, the pile transitions to `SYNCHRONIZED`.

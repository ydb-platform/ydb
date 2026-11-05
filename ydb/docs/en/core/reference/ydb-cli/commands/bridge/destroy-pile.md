# admin cluster bridge destroy-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge destroy-pile` command to delete a disconnected pile's storage-group parts together with all their data and make the pile `INACTIVE`. This operation is irreversible.

{% include [danger-warning](../_includes/danger-warning.md) %}

{% note warning %}

Before running the command, ensure that no ordinary group has VDisks in the pile and no bridged group depends solely on its part there.

{% endnote %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge destroy-pile [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge destroy-pile --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--pile <pile>` | Name of the `DISCONNECTED` pile to destroy. |

## Requirements {#requirements}

- The pile must be `DISCONNECTED`, and its nodes must be stopped. If it is `PRIMARY`, perform switchover first and then takedown.
- No ordinary group may have VDisks in the pile.
- No bridged group may depend solely on its part in the pile.
- No other pile addition or removal can be in progress, except a pending `ADD` of the same pile, which this command cancels.

For the complete procedure, see [Removing a pile](../../../../devops/deployment-options/manual/bridge-management.md#remove-pile).

## Examples {#examples}

Destroy disconnected pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge destroy-pile --pile pile-b
```

Without the global `-y` option, the command asks for confirmation before deleting data.

### Verifying the result {#verify}

The command starts a pending `REMOVE` operation. Use [list](list.md) to wait until this operation clears and the pile becomes `INACTIVE`, after which the pile and its hosts can be removed from the configuration. A removed pile can later be registered and [provisioned](provision-pile.md) again on fresh or the same hosts.

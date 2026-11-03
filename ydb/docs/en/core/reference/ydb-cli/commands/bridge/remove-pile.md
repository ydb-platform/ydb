# admin cluster bridge remove-pile

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge remove-pile` command to delete a disconnected pile's parts of every storage group together with all their data and make the pile `INACTIVE`. This operation is irreversible.

{% include [danger-warning](../_includes/danger-warning.md) %}

{% note warning %}

Removing a pile deletes all data held by its storage group parts. Before running the command, ensure that no ordinary group has VDisks in the pile and no bridged group relies solely on its part there.

{% endnote %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge remove-pile [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge remove-pile --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--pile <pile>` | Name of the `DISCONNECTED` pile to remove. |

## Requirements {#requirements}

- The pile must be `DISCONNECTED`, and its nodes must be stopped. If it is `PRIMARY`, perform switchover first and then takedown.
- No ordinary group may have VDisks in the pile.
- No group may rely solely on its part in the pile. Wait for pool conversion to synchronize or retain another pile when making a pool ordinary.
- No other pile addition or removal can be pending.

The command also cancels a stuck pending `ADD` of the specified pile. For data-safety checks and the complete procedure, see [Removing a pile](../../../../devops/deployment-options/manual/bridge-management.md#remove-pile).

## Examples {#examples}

Remove disconnected pile `pile-b`:

```bash
{{ ydb-cli }} -y admin cluster bridge remove-pile --pile pile-b
```

Without a TTY, omitting the global `-y` option silently declines confirmation and exits with code 1.

### Verifying the result {#verify}

Check the pending operation:

```bash
{{ ydb-cli }} admin cluster bridge list
```

BSC deletes the pile's parts and data during the pending `REMOVE` operation. Wait for it to clear and for the pile to become `INACTIVE` before removing the pile and its hosts from `bridge_config`, `hosts`, and `system_tablets`.

A removed pile can later be registered and [added](add-pile.md) again on fresh or the same hosts.

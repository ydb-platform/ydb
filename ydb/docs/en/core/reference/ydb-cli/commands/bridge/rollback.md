# admin cluster bridge rollback

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge rollback` command to return a cluster previously [converted from ordinary to bridge mode](convert.md) to ordinary mode while preserving its data. You can use the command after conversion completes or when conversion is stuck or only partially complete.

The command makes static storage groups ordinary, after which the storage controller makes dynamic groups and pools ordinary, and then the bridge cluster state is removed. You must then manually remove `bridge_config` and pile names from the configuration and restart the nodes without them.

{% include [danger-warning](../_includes/danger-warning.md) %}

{% note warning %}

Successful completion of the command means that static groups have become ordinary and rollback of the remaining storage has started; it does not mean that the cluster has fully returned to ordinary mode. For the complete procedure, see [Returning a converted cluster to ordinary mode](../../../../devops/deployment-options/manual/bridge-management.md#rollback).

{% endnote %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge rollback [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge rollback --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--skip-bsc-validation` | Skip preliminary validation of dynamic groups by the storage controller. Use only when the controller cannot start after conversion because it depends on static groups. This option does not disable rollback validation: [list](list.md) shows blocking conditions, and attempts continue automatically. |

## Requirements {#requirements}

- The cluster must have been converted from ordinary to bridge mode. Rollback is unavailable for a cluster originally deployed in bridge mode with multiple piles or after its original pile has been removed.
- The original pile must be the only pile remaining in the cluster and in `bridge_config`; in this state, it can only be `PRIMARY` and is therefore synchronized. [Remove other piles](../../../../devops/deployment-options/manual/bridge-management.md#remove-pile) beforehand; `rollback` does not remove them itself.
- No other pile operations or pile synchronization can be in progress. The current conversion to bridge mode does not have to be complete.
- Every bridged group must be an inline group in the original pile: it must have its own VDisks and a synchronized part there and no parts in other piles. Such groups must not be undergoing decommission.
- Rules for the original pile must be removed from `blob_storage_config.storage_pool_pile_overrides`.
- Automatic configuration management must be enabled. Every static and connected dynamic node must run a {{ ydb-short-name }} build that supports bridge rollback, and every configured static node must report its configuration.

If preliminary validation finds an unsupported group, the command returns an error with its group ID and does not start rollback. Rollback does not restore a configuration from an old snapshot, undo migration to configuration V2, or replace the current {{ ydb-short-name }} version with an earlier one.

## Examples {#examples}

Return a converted cluster to ordinary mode:

```bash
{{ ydb-cli }} -y admin cluster bridge rollback
```

Without the global `-y` option, the command asks for confirmation.

If the storage controller cannot start after conversion:

```bash
{{ ydb-cli }} -y admin cluster bridge rollback --skip-bsc-validation
```

### Verifying the result {#verify}

Use `admin cluster bridge list piles` to wait for storage rollback to complete:

```bash
{{ ydb-cli }} admin cluster bridge list piles
```

While rollback is in progress, the output contains:

```text
Rollback: storage in progress
```

If the last attempt failed, the output contains `Last attempt has failed (being retried): <error>`. Resolve the specified cause; the storage controller retries automatically. Calling `rollback` again while rollback is already in progress does not create another operation.

After the bridge cluster state is removed, `list` reports `Bridge configuration still in use` and shows how many nodes still run with `bridge_config`. At this stage, remove the bridge settings and restart the nodes using the [complete procedure](../../../../devops/deployment-options/manual/bridge-management.md#rollback). When rollback is complete, the command prints `Cluster mode: ordinary`.

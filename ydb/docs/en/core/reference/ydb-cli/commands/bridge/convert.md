# admin cluster bridge convert

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge convert` command to convert a non-bridged cluster into a [bridge cluster](../../../../concepts/bridge.md) with one pile. Conversion is irreversible.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge convert [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge convert --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--skip-storage-pool-conversion` | Leave existing storage pools and groups ordinary. Static and system storage is still converted. Existing pools can later be converted separately with [`ydb-dstool pool set-bridge-mode`](../../../ydb-dstool/pool-set-bridge-mode.md). This option requires `bridge_config.enable_non_uniform_storage: true`. |

## Requirements {#requirements}

- Configuration V2 and self-management must be enabled.
- Every static and dynamic node must run a supporting build and must have been restarted with a single-pile `bridge_config`.
- The configuration must contain exactly one pile.
- With `--skip-storage-pool-conversion`, `bridge_config.enable_non_uniform_storage` must be `true`. With default conversion, it must be unset until conversion completes.

For the complete checklist and configuration examples, see [Converting a cluster](../../../../devops/deployment-options/manual/bridge-management.md#convert).

## Examples {#examples}

Convert the cluster and make every existing pool bridged:

```bash
{{ ydb-cli }} -y admin cluster bridge convert
```

Convert only system storage and leave existing pools ordinary:

```bash
{{ ydb-cli }} -y admin cluster bridge convert --skip-storage-pool-conversion
```

These are alternative modes. Choose the second for large clusters or when ordinary pools are required, so pool synchronization can be controlled separately.

### Verifying the result {#verify}

Use [list](list.md) to wait until conversion is complete:

```bash
{{ ydb-cli }} admin cluster bridge list
```

The conversion is still running while the output contains `Conversion from non-bridged cluster is in progress, piles can't be added yet`. After completion, the single pile is shown as `PRIMARY`. Until then, the pile list cannot change.

# pool set-bridge-mode

Use the `pool set-bridge-mode` subcommand to change a storage pool in a bridge cluster between [bridged and ordinary modes](../../concepts/bridge.md#storage-pools) while preserving group IDs and data.

General command syntax:

```bash
ydb-dstool [global options ...] pool set-bridge-mode [options ...]
```

- `global options` — [global options](global-options.md);
- `options` — [subcommand options](#options).

View command help:

```bash
ydb-dstool pool set-bridge-mode --help
```

## Subcommand options {#options}

| Option | Description |
| --- | --- |
| `--pool-name <database:kind>` | Storage pool to change, for example `/Root/db1:ssd`. Repeat this option to change several pools in one request. This option is required. |
| `--bridged` | Make the pool bridged. Every group receives a part in every pile, synchronized in the background. Mutually exclusive with `--ordinary`. |
| `--ordinary` | Make the pool ordinary. Every bridged group keeps one part, and the other parts are deleted. Mutually exclusive with `--bridged`. |
| `--retain-pile <name>` | With `--ordinary`, the pile whose part each group keeps. By default, a group keeps the part it was made bridged from, its inline pile, or its part in the primary pile. |
| `--format <FORMAT>` | Output format: `pretty` (default) or `json`. |

Exactly one of `--bridged` and `--ordinary` is required. `--retain-pile` can be used only with `--ordinary`.

## Requirements {#requirements}

- The cluster must be converted to bridge mode, conversion must be complete, and `bridge_config.enable_non_uniform_storage` must be enabled.
- No pile addition or removal can be pending.
- To make a pool bridged, every group must reside in one pile. Cross-data-center `mirror-3-dc` groups cannot be converted directly.
- To make a pool ordinary, the retained part must be `SYNCED` and its pile must remain in the data quorum. A non-authority retained part also requires its pile to be `SYNCHRONIZED`.

For operational restrictions and synchronization safety, see [Changing a storage pool mode](../../devops/deployment-options/manual/bridge-management.md#pool-mode).

## Examples {#examples}

Make pool `/Root/db1:ssd` bridged:

```bash
ydb-dstool pool set-bridge-mode --pool-name /Root/db1:ssd --bridged
```

New parts are created immediately and synchronized in the background. Until synchronization completes, each group depends only on its original pile. Do not disconnect that pile during synchronization.

Make the same pool ordinary and retain its parts in `pile-a`:

```bash
ydb-dstool pool set-bridge-mode --pool-name /Root/db1:ssd --ordinary --retain-pile pile-a
```

Other parts are deleted in the background, approximately 30 seconds later. Afterwards, the pool is unavailable whenever `pile-a` is disconnected, and that pile cannot be removed.

Use [bridge list](../ydb-cli/commands/bridge/list.md) to check pile states and BSC synchronization counters to track progress. `TDefineStoragePool` cannot change the mode of an existing pool; use this command instead.

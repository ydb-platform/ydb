# admin cluster bridge set-database-mode

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge set-database-mode` command to switch all storage pools of a database between bridged and ordinary storage. Existing data is preserved. Replication or deletion of redundant copies continues in the background after the command returns.

{% include [danger-warning](../_includes/danger-warning.md) %}

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge set-database-mode <mode> --target-database <path> [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `<mode>` — `bridged` to enable bridge replication or `ordinary` to disable it;
- `<path>` — absolute path of the database to change;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge set-database-mode --help
```

## Subcommand parameters {#options}

| Name | Description |
| --- | --- |
| `--target-database <path>` | Required absolute path of the database to change. It does not change the connection or authentication database set by the global `--database` option. |
| `--retain-pile <pile>` | Only when switching to `ordinary`: retain parts of all converted groups in the specified pile. For details, see [Choosing retained copies](#retain-pile). |

## Requirements {#requirements}

- The cluster must be converted to bridge mode, and conversion must be complete.
- `bridge_config.enable_non_uniform_storage: true` must be set.
- No pile addition or removal can be in progress.
- When switching to `bridged`, every existing group must reside within a single pile. Groups spanning several piles cannot be converted directly.
- `--retain-pile` can be used only with `ordinary`.
- The pile selected with `--retain-pile` must contain synchronized parts of every group being converted to `ordinary`.

For a serverless database, specify its shared database as `--target-database`. The change applies to the shared database and all its serverless databases.

All database pools are switched atomically: if validation fails, no pool mode is changed. After a successful switch to `ordinary`, group parts outside the retained piles are deleted in the background.

For the complete procedure, see [Switching a database storage mode](../../../../devops/deployment-options/manual/bridge-management.md#database-mode).

## Choosing retained copies when switching to ordinary storage {#retain-pile}

Each bridged group has one part in every participating pile. When switching to `ordinary`, one part is selected for each such group; this part is retained and becomes an ordinary group. The other parts are deleted in the background. Pools already using ordinary storage are not changed: the command does not relocate their data to another pile.

With `--retain-pile <pile>`, all converted groups in all database pools retain their parts in the single specified pile. You can select `PRIMARY` or another pile in the `SYNCHRONIZED` state; its role does not change. The selected pile must contain a `SYNCED` part of every group being converted. If this condition is not met for even one group, the command returns an error and no pool's mode changes. Use this option when data placement must be explicit, for example before removing another pile.

Without `--retain-pile`, {{ ydb-short-name }} selects the retained part independently for each group: the previously designated retained part; if none is designated, the original inline part from which the bridged group was created; if there is no such part, the part in `PRIMARY`. The selected part must also be `SYNCED`. Consequently, after previous mode changes, different groups of the same database may remain in different piles: automatic selection does not consolidate all database data into one pile.

Before switching, check the state of the group parts as described in [Verifying the result](#verify).

## Examples {#examples}

Switch database `/Root/db1` to bridged storage:

```bash
{{ ydb-cli }} -y admin cluster bridge set-database-mode bridged --target-database /Root/db1
```

Switch database `/Root/db1` to ordinary storage and retain data in `pile-a`:

```bash
{{ ydb-cli }} -y admin cluster bridge set-database-mode ordinary --target-database /Root/db1 --retain-pile pile-a
```

### Verifying the result {#verify}

Successful completion of the command confirms the atomic mode change of all database pools, not the completion of background synchronization or cleanup. The `bridge list` command does not show these operations, and pile states may remain unchanged.

To verify synchronization of bridged group parts or deletion of redundant parts after switching to ordinary storage, use the `.sys/ds_groups` system view as described in [Verifying a database mode change](../../../../devops/deployment-options/manual/bridge-management.md#verify-database-mode).

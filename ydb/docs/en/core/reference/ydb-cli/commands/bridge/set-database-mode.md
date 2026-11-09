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
| `--retain-pile <pile>` | With `ordinary`, retain group parts in the specified pile. You can select `PRIMARY` or another pile in the `SYNCHRONIZED` state; the pile's role does not change. If omitted, retained copies are selected automatically and may be in different piles for different groups. This option does not relocate data in pools that are already ordinary. |

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

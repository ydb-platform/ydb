# admin cluster bridge list

{% include [feature_enterprise.md](../../../../_includes/feature_enterprise.md) %}

Use the `admin cluster bridge list` command to list pile states, cluster conversion and rollback progress, pending pile operations, and database and storage-group synchronization in [bridge mode](../../../../concepts/bridge.md). This command is read-only and does not ask for confirmation.

General command syntax:

```bash
{{ ydb-cli }} [global options...] admin cluster bridge list [piles|databases|groups] [options...]
```

- `global options` — [global parameters](../global-options.md) for the CLI;
- `piles|databases|groups` — [view](#views), defaulting to `piles`;
- `options` — [subcommand parameters](#options).

View command help:

```bash
{{ ydb-cli }} admin cluster bridge list --help
```

## Subcommand parameters {#options}

#|
|| Name | Description ||
|| `--format <pretty, json, csv>` | Output format. Valid values: `pretty`, `json`, `csv`. Default: `pretty`. ||
|| `--detailed` | Show additional information: database counts for `piles`, group counts and the last error for `databases`, and physical IDs, phases, and the last error for `groups`. The `databases` and `groups` JSON output includes available details by default. ||
|| `--pile <name>` | Show only the specified pile. An empty name is valid. ||
|| `--database-path <path>` | Only for `databases` and `groups`: select a database by its absolute path. A serverless database path selects its shared storage. ||
|| `--not-synced` | Only for `databases` and `groups`: show only incomplete required synchronization, including unknown states. ||
|| `--mode <mode>` | Only for `databases`: filter by the mode of all database pools: `bridged`, `ordinary`, or `mixed`. ||
|| `--pool <name>` | Only for `groups`: select a uniquely named storage pool. Mutually exclusive with `--database-path` and `--pool-id`. ||
|| `--pool-id <box:pool>` | Only for `groups`: select a storage pool by its `BOX:POOL` IDs. Mutually exclusive with `--database-path` and `--pool`. ||
|#

## Views {#views}

### piles {#piles}

The state of each pile, conversion or rollback progress, and pending pile operations. This is the default view. With `--detailed`, it also shows a database synchronization summary by pile.

### databases {#databases}

Storage mode and group synchronization summary for each database and pile. The `mixed` mode means that the database has both ordinary and bridged storage pools. With `--detailed`, the view shows synchronization counters and the last error. For output states and completion criteria, see [Verifying a database mode change](../../../../devops/deployment-options/manual/bridge-management.md#verify-database-mode).

### groups {#groups}

Synchronization state of storage-group parts by pile. With `--detailed`, it shows physical group IDs, synchronization phases, and the last error. For output states and completion criteria, see [Verifying a database mode change](../../../../devops/deployment-options/manual/bridge-management.md#verify-database-mode).

## Examples {#examples}

List piles in human-readable format:

```bash
{{ ydb-cli }} admin cluster bridge list
```

```text
Pile pile-a: PRIMARY
Pile pile-b: SYNCHRONIZED
Generation: 4
```

During conversion, the output also contains `Conversion from non-bridged cluster is in progress, piles can't be added yet`. During [rollback to ordinary mode](rollback.md), it contains `Rollback: storage in progress`; after storage rollback completes, it reports `Bridge configuration still in use` until the nodes are restarted without `bridge_config`. During pile provisioning or removal, it contains the pending operation and, after a failed attempt, its error:

```text
Pending operation on pile pile-b: ADD
Last attempt has failed (being retried): <error>
```

Output state in JSON format:

```bash
{{ ydb-cli }} admin cluster bridge list --format json
```

```json
{
  "generation": 4,
  "piles": [
    {
      "pile_name": "pile-a",
      "state": "PRIMARY"
    },
    {
      "pile_name": "pile-b",
      "state": "SYNCHRONIZED"
    }
  ],
  "conversion_in_progress": false
}
```

If a pile operation is pending, the JSON output additionally contains `pending_pile_operation` with `pile_name`, `kind`, and `error_reason` fields.

Output state in CSV format:

```bash
{{ ydb-cli }} admin cluster bridge list --format csv
```

```csv
pile_name,state,generation
pile-a,PRIMARY,4
pile-b,SYNCHRONIZED,4
```

Show a detailed synchronization summary for database `/Root/db1` by pile:

```bash
{{ ydb-cli }} admin cluster bridge list databases --database-path /Root/db1 --detailed
```

Show only this database's group parts with incomplete synchronization:

```bash
{{ ydb-cli }} admin cluster bridge list groups --database-path /Root/db1 --not-synced
```

An empty result with `--not-synced` means that the selected data has no incomplete required synchronization. To see all parts, including synchronized parts and those that do not require synchronization, omit this option.

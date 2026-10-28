# Initial Synchronization for Asynchronous Replication Using Backup and Restore

With this initial synchronization method, the source data is transferred to the target from a backup in S3-compatible storage. [Asynchronous replication](../concepts/async-replication.md) then applies accumulated changes and continues synchronizing the databases. Applications can keep writing to the source during the transfer.

This method uses server-side [backup](../reference/ydb-cli/export-import/export-s3.md) and [restore](../reference/ydb-cli/export-import/import-s3.md) operations to transfer large datasets. With an [initial CDC scan](../concepts/async-replication.md#initial-scan), all source data passes through changefeeds, and a separate backup is not required. For differences in resource and operational costs, see the [comparison of initial synchronization methods](../concepts/async-replication.md#initial-sync-comparison).

## Prerequisites {#prerequisites}

In this example, the `original_table` table in the `/Root/source` database is transferred to the `replica_table` table in the `/Root/target` database. The `my_replication` replication instance is created on the target. The commands use the `source` and `target` [{{ ydb-short-name }} CLI profiles](../reference/ydb-cli/profile/index.md) to connect to the respective databases.

Before you begin:

- Configure the profiles and check connectivity to both databases using the [`scheme ls` command](../reference/ydb-cli/commands/scheme-ls.md).
- Set up [permissions for asynchronous replication](../concepts/async-replication.md#init), export on the source, and import on the target. The user creating the replication instance must be able to alter the restored tables to attach them as replicas.
- On the target, [create a secret](../yql/reference/syntax/create-secret.md) named `source_token` containing the token that replication will use to connect to the source.
- Prepare the `mybucket` bucket and [S3 authentication](../reference/ydb-cli/export-import/auth-s3.md). The examples use the `storage.yandexcloud.net` endpoint and read access keys from environment variables or the `~/.aws/credentials` file. The endpoint must be accessible from the servers of both databases: the source writes the backup, and the target reads it.
- Choose an unused prefix, `replication-bootstrap`, in the bucket. Do not modify the exported files until the import is complete.
- Make sure that `replica_table` does not exist on the target. The `import s3` command creates the table together with its data and does not overwrite existing tables. Use `scheme ls` to check the path.

Keep the set of replicated tables, their columns, and their indexes unchanged during initial synchronization. The table schemas in the backup must match the source schemas when replication starts.

{% note warning %}

Records in [changefeeds](../concepts/cdc.md) have a limited retention period. Plan the export, import, and processing of accumulated changes so that replication can read the records before they are deleted. The default retention period is 24 hours; check the actual value after creating the changefeeds. The `PAUSED` state does not suspend the retention period.

{% endnote %}

## 1. Create Replication Without an Initial Scan {#create-replication}

On the target, run a [CREATE ASYNC REPLICATION](../yql/reference/syntax/create-async-replication.md) statement with `INITIAL_SCAN = FALSE`. You can run the YQL statements in this guide using the [`sql` command](../reference/ydb-cli/sql.md) with the `target` profile.

```yql
CREATE ASYNC REPLICATION my_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://source.example.com:2135/?database=/Root/source',
    TOKEN_SECRET_PATH = 'source_token',
    INITIAL_SCAN = FALSE
);
```

The `source.example.com:2135` address in the example is the source endpoint. The `CONNECTION_STRING` and `TOKEN_SECRET_PATH` parameters specify the connection that the target will use to create and read changefeeds.

Replication is created in the `PAUSED` state and prepares changefeeds on the source without exporting the initial data. The `replica_table` table is not created or attached to replication yet. The import operation will create it.

## 2. Wait for the Changefeeds to Be Ready {#wait-for-changefeeds}

Changefeeds are created asynchronously. Before starting the export, check that they are ready for all replicated tables, including index implementation tables if they are part of the replication.

Get the [replication description](../reference/ydb-cli/commands/scheme-describe.md) in JSON format:

```bash
{{ ydb-cli }} -p target scheme describe my_replication --format proto-json-base64
```

Check the following fields:

- `paused`: replication is waiting to be started after the import. This field alone does not confirm that the changefeeds are ready.
- `items`: the list of objects with their `source_path` and `destination_path` paths. Wait until all expected objects appear.
- `source_changefeed_name` for each object: the changefeed name. Names are assigned automatically; do not set them manually.

For `original_table`, check the changefeed state on the source:

```bash
{{ ydb-cli }} -p source scheme describe original_table --format proto-json-base64
```

In the `changefeeds` array, find the changefeed with the name from `source_changefeed_name`. Its `state` field must be `STATE_ENABLED`.

For an item corresponding to a synchronous or unique index, `source_path` points to the index. Its implementation table is located at `source_path/indexImplTable`. Check the `changefeeds` array in the description of that table.

Also use [`scheme describe`](../reference/ydb-cli/commands/scheme-describe.md) to check the changefeed topic settings: the `replicationConsumer` consumer must exist, and `RetentionPeriod` must have a suitable value. The changefeed path depends on the object:

| Object | Changefeed path |
| --- | --- |
| Table | `source_path/source_changefeed_name` |
| Index implementation table | `source_path/indexImplTable/source_changefeed_name` |

Use the field values from the replication description in these paths. To check the settings, run:

```bash
{{ ydb-cli }} -p source scheme describe <changefeed-path>
```

Where:

- `<changefeed-path>`: the changefeed path, for example, `/Root/source/original_table/stream-name`. Replace `stream-name` with the name from the replication description.

Repeat the checks for every object in `items`. If the retention period is too short, increase it before a lengthy transfer using [ALTER TOPIC](../yql/reference/syntax/alter-topic.md#alter-topic). Changing the retention period does not recover records that have already been deleted.

{% note warning %}

Start the export only after all changefeeds are ready. If the backup is created earlier, replication may miss changes made between the backup and the creation of the changefeeds. You cannot use an arbitrary pre-existing backup for this procedure.

{% endnote %}

## 3. Export the Source Data {#export}

Start an [export to S3](../reference/ydb-cli/export-import/export-s3.md) using the source profile:

```bash
{{ ydb-cli }} -p source export s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket mybucket \
  --destination-prefix replication-bootstrap \
  --include original_table \
  --include-index-data true
```

The table export is stored under the `replication-bootstrap/original_table` prefix. The `--include-index-data true` option also saves index table data if the source table has indexes. This data will be loaded from the backup during the import.

For multiple tables, specify `--include` for each table in a single export operation: the server creates a consistent copy of the tables. The exported objects must match the set of replication objects.

The command returns a server-side operation ID. Wait for the operation to complete successfully using [`operation get`](../reference/ydb-cli/export-import/export-s3.md#status):

```bash
{{ ydb-cli }} -p source operation get <export-operation-id>
```

Where:

- `<export-operation-id>`: the ID returned when you started the export.

Proceed to the import only when the table output shows `ready = true`, `status = SUCCESS`, and `progress = Done`. Successfully starting the export command does not mean that the export has finished.

Applications can continue changing the source data. Changefeeds retain these changes while replication is in the `PAUSED` state.

## 4. Import the Data into the Target {#import}

After the export succeeds, start an [import from S3](../reference/ydb-cli/export-import/import-s3.md) using the target profile:

```bash
{{ ydb-cli }} -p target import s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket mybucket \
  --item src=replication-bootstrap/original_table,dst=replica_table \
  --index-population-mode import
```

The `--item` option maps the table prefix in the backup to the `replica_table` target path specified in `CREATE ASYNC REPLICATION`. The `--index-population-mode import` mode loads the index data saved in the previous step. For multiple tables, specify a separate `--item` for each pair of paths.

Wait for the operation to complete successfully using [`operation get`](../reference/ydb-cli/export-import/import-s3.md#status):

```bash
{{ ydb-cli }} -p target operation get <import-operation-id>
```

Where:

- `<import-operation-id>`: the ID returned when you started the import.

Make sure that `ready = true`, `status = SUCCESS`, and `progress = Done`. Check the created table:

```bash
{{ ydb-cli }} -p target scheme describe replica_table
```

Until replication starts, the restored table is a regular table. Do not write application data to it: these writes are not synchronized with the source and may be overwritten when changes are applied.

## 5. Start Change Data Replication {#start-replication}

After all table imports are complete, run [ALTER ASYNC REPLICATION](../yql/reference/syntax/alter-async-replication.md) on the target:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'STANDBY');
```

The target checks that the restored table schemas match the source schemas and attaches the tables as replica objects. The check covers the primary key, columns, their types, column families, and indexes. Attached tables become read-only, and replication starts applying accumulated changes.

Records are read from the changefeeds created before the export. They may include changes already reflected in the backup. Until the accumulated records have been processed, the target data may change and does not yet match the current source state.

Check the state and lag:

```bash
{{ ydb-cli }} -p target scheme describe my_replication --stats
```

After replication starts, the `paused` field in the description is replaced by `running`. Statistics include lag for replication and individual objects. Wait until the accumulated changes have been processed; a missing lag value does not mean zero lag.

Replication then continues transferring new changes. If the transfer is part of an application migration, follow the separate procedure for [completing asynchronous replication](../concepts/async-replication.md#done) before switching writes to the target.

## Errors and Retries {#errors}

- If an export or import fails, check its status and error description using `operation get`. Keep replication in the `PAUSED` state until all table imports succeed. When retrying an import, remember that target tables must not exist; a failed operation does not confirm that no objects were created.
- If starting replication fails, get its JSON description and check the `error` field. In particular, check that all target tables exist, their schemas match, and permissions are sufficient. Do not import over tables already attached as replicas.
- If required CDC records have been deleted because their retention period expired, continuing to read cannot ensure data completeness. Repeat initial synchronization with new changefeeds and a new backup.
- Restarting the replication controller preserves the `PAUSED` state; it does not replace an explicit transition to `STANDBY`. If a CLI session is interrupted, check previously started server-side operations by their IDs before starting new ones.

For more information about temporary and critical errors, see [Error Handling During Asynchronous Replication](../concepts/async-replication.md#error-handling).

# Migrating a Database Using Asynchronous Replication

This recipe transfers a row-oriented table between {{ ydb-short-name }} databases using [asynchronous replication](../../concepts/async-replication.md), then switches the application to the target. The application continues using the source during the initial transfer. Writes must stop for final verification and cutover.

{% note warning %}

This step-by-step example applies to a table **without secondary indexes**. A replicated index can be processed separately from the base table, so matching base-table exports do not establish index readiness. Before migrating tables with indexes, agree on a separate procedure that fully compares every record of each replicated index, including keys and covering columns. Do not treat the base-table check below as authorization to run `DONE/FORCE` for such a migration.

{% endnote %}

Choose an [initial synchronization method](index.md#initial-sync) before creating replication. The alternatives below use the same object names; perform only one of them.

## Example Objects {#input-data}

| Object | Example name |
| --- | --- |
| Source database and CLI profile | `/Root/source`, `source` |
| Target database and CLI profile | `/Root/target`, `target` |
| Source table | `original_table` |
| Target table | `replica_table` |
| Replication instance on the target | `my_replication` |
| Secret on the target | `source_token` |

Use your own values consistently throughout the procedure. Replication management statements run on the target; changefeed checks run on the source.

## Prerequisites {#prepare}

- Configure the `source` and `target` [CLI profiles](../../reference/ydb-cli/profile/index.md), including endpoints, database paths, and authentication.
- Record the tables to transfer. New tables are not automatically added to an existing replication instance. The target paths must be unused.
- Prepare application permissions and all objects outside the [scope of replication](../../concepts/async-replication.md#restrictions) separately. Distinguish restoring topic definitions from transferring their messages. Check support for the required indexes and sequences in both databases.
- Check resource headroom on both databases and space for CDC on the source. Choose the [availability period for accumulated changes](../../concepts/async-replication.md#availability-period), data verification queries, and an acceptable write downtime window.

### Permissions {#replication-permissions}

Configure permissions on the target database root and directories separately before application cutover. For [GRANT permission names](../../yql/reference/syntax/grant.md#permissions-list), use the following checklist:

| Purpose | Access |
| --- | --- |
| Connect and inspect objects | `CONNECT` on each database; `DESCRIBE SCHEMA` on the directories and objects used |
| Replication access to the source | `ALTER SCHEMA` on source tables and changefeed topics, and `SELECT ROW` on topics, for the user whose token replication uses. The user of the `source` CLI profile also needs these permissions when configuring changefeeds manually |
| Create target objects | `CREATE TABLE` on the target directory; `CREATE DIRECTORY` on the parent when subdirectories are needed |
| Manage replication | `ALTER SCHEMA` on the replication instance; `REMOVE SCHEMA` to delete it; for restored tables, `ALTER SCHEMA` for the replication owner |
| Read the secret | `SELECT ROW` on `source_token` for the replication owner |
| Verify data | `SELECT ROW` on the source and target tables for the users performing verification |
| Application access to the target | `SELECT ROW`, `UPDATE ROW`, and `ERASE ROW` as required by the application |

For the backup method, also prepare the export and import permissions described in the [initial synchronization guide](../async-replication-initial-sync.md#prerequisites).

## 1. Prepare the Connection and Secret {#setup}

### Check Connectivity {#check-connection}

```bash
{{ ydb-cli }} -p source scheme describe original_table
{{ ydb-cli }} -p target scheme ls
```

The first command must describe the expected source table; the second must list target objects. Resolve connection or permission errors before proceeding.

### Obtain a Token {#token}

Prepare a token for a source user with the [required permissions](#replication-permissions), using the [authentication method](../../security/authentication.md) configured for your installation. Keep it in a protected file and out of command histories and logs.

### Create a Secret {#secret}

Save a [CREATE SECRET](../../yql/reference/syntax/create-secret.md) statement in a protected `create-secret.sql` file:

```yql
CREATE SECRET source_token
WITH (value = '<source-token>');
```

Replace `<source-token>` with the prepared source token. Run the file **on the target**:

```bash
{{ ydb-cli }} -p target sql -f create-secret.sql
```

Verify that the replication owner can read the secret. Remove the local file according to your team's credential handling rules.

## 2. Create Replication with an Initial Scan {#create}

Run this section only for `INITIAL_SCAN = TRUE`. For backup-based synchronization, go directly to [section 3.2](#export-import-sync); that guide creates its own replication instance.

Save the following [CREATE ASYNC REPLICATION](../../yql/reference/syntax/create-async-replication.md) statement in `create-replication.sql`:

```yql
CREATE ASYNC REPLICATION my_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://source.example.com:2135/?database=/Root/source',
    TOKEN_SECRET_PATH = 'source_token',
    CONSISTENCY_LEVEL = 'ROW',
    INITIAL_SCAN = TRUE
);
```

Choose the table grouping using the [planning recommendations](index.md#replication-scope). For multiple tables, list all source and target pairs in one `FOR` clause.

Replace the example endpoint and database path with the source connection settings. This example uses row-level consistency; see [consistency guarantees](../../concepts/async-replication.md#consistency-levels). Choose the [availability period](../../concepts/async-replication.md#availability-period) before creation.

```bash
{{ ydb-cli }} -p target sql -f create-replication.sql
{{ ydb-cli }} -p target scheme describe my_replication --stats --format proto-json-base64
```

Check the result:

| Field | Expected result |
| --- | --- |
| `items` | All selected objects with the expected `source_path` and `destination_path` |
| `items[].source_changefeed_name` | The generated changefeed name for every object; wait until populated |
| `running` | Present for an initial CDC scan |
| `paused` | Present with `INITIAL_SCAN = FALSE`; replication awaits restored data |
| `error` | Absent |

Resolve any reported error before proceeding. On `UNIMPLEMENTED`, agree with your installation's support team on an available way to inspect the object: an interface error does not prove that the object is absent. Do not repeat `CREATE` to diagnose it.

## 3. Transfer the Initial Data {#initial-sync}

### 3.1. Initial CDC Scan {#initial-scan}

For `INITIAL_SCAN = TRUE`, the scan is already running. Check both databases:

```bash
{{ ydb-cli }} -p target scheme describe my_replication --stats --format proto-json-base64
{{ ydb-cli }} -p source scheme describe original_table --format proto-json-base64
```

Replication must remain `running` without `error`. Check scanning progress for every object. In the source table description, the changefeed transitions from `STATE_INITIAL_SCAN` to `STATE_ENABLED` after scanning completes. Verify that target tables exist.

A completed scan does not prove that accumulated changes have been applied. Continue with [lag assessment](#replication-lag).

### 3.2. Synchronization Using a Backup {#export-import-sync}

For the backup method, **do not run section 2 above**. First complete the [initial synchronization prerequisites](../async-replication-initial-sync.md#prerequisites): prepare S3 storage, access credentials, an unused prefix, and the change availability period. The `source_token` secret was already created in section 1; reuse it instead of creating a second secret. Then complete steps 1–5 of the same guide: create replication with `INITIAL_SCAN = FALSE`, verify CDC readiness, export, import, and start change replication.

Use the same names: `my_replication`, `replica_table`, `source_token`, `original_table`, and the `source` and `target` profiles. After the instance enters `running`, return to [lag assessment](#replication-lag) below.

Individual steps in the guide:

- [Check changefeed readiness](../async-replication-initial-sync.md#wait-for-changefeeds). {#s3-changefeeds}
- [Export the source data](../async-replication-initial-sync.md#export). {#s3-export}
- [Import into the target](../async-replication-initial-sync.md#import). {#s3-import}
- [Start applying changes](../async-replication-initial-sync.md#start-replication). {#s3-start-replication}

### 3.3. Assess Replication Lag {#replication-lag}

```bash
{{ ydb-cli }} -p target scheme describe my_replication --stats --format proto-json-base64
```

Check `running.stats.lag` and each `items[].stats.lag`; the `error` field must be absent. Durations such as `5s` mean five seconds. Repeat the check: lag should decrease and remain small enough for the agreed write downtime window. A missing value does not mean zero lag; even `0s` does not replace [final verification](#readiness).

### 3.4. Errors and Stopped Replication {#replication-overload}

If `error` appears, save its message and check source availability, permissions, and schemas. If lag grows, check resources on both databases and CDC storage on the source. Do not start cutover until the cause is resolved.

After resolving a critical error, [pause and resume](../../yql/reference/syntax/alter-async-replication.md) replication on the target:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'PAUSED');
ALTER ASYNC REPLICATION my_replication SET (STATE = 'STANDBY');
```

Get the description again. Continue only after `running` appears without `error` and you have confirmed that required CDC records remain available. If the history has been lost, repeat initial synchronization.

## 4. Verify Readiness for Cutover {#readiness}

This procedure requires an available source. Otherwise, use the [source failure guidance](#source-unavailable).

### Stop Writes {#stop-writes}

Stop source writes from applications, background jobs, loaders, and retry deliveries. Wait for active transactions to finish and prevent old writers from restarting. **Write downtime starts here.**

### Check Applied Changes {#check-replication}

Use `running.stats.lag` and `items[].stats.lag` to monitor the transfer. The response provides neither a measurement time nor a report number: a repeated `0s` does not establish that a new report arrived after source writes stopped. A missing value does not mean zero lag. Determine readiness using [full data comparison](#check-data), rather than a lag value alone.

Use `source_path` and `source_changefeed_name` from each entry in `items`:

| Object | CDC topic path |
| --- | --- |
| Table | `source_path/source_changefeed_name` |
| Index implementation table | `source_path/indexImplTable/source_changefeed_name` |

For an index, inspect the table and its changefeed state at `source_path/indexImplTable`, as described in the [changefeed readiness checks](../async-replication-initial-sync.md#wait-for-changefeeds). Inspect every topic and its replication consumer:

```bash
{{ ydb-cli }} -p target scheme describe my_replication --stats --format proto-json-base64
{{ ydb-cli }} -p source scheme describe <changefeed-path> --stats --partition-stats
{{ ydb-cli }} -p source topic consumer describe <changefeed-path> --consumer <replication-consumer> --partition-stats
```

Where:

- `<changefeed-path>` is the source topic path, for example `/Root/source/original_table/stream-name`; use the generated stream name.
- `<replication-consumer>` is the consumer name from the topic description.

`Start offset` is the start of the range of messages still available; `End offset` is the end of the written range, excluding the boundary itself; `Committed offset` is the boundary whose processing the consumer has acknowledged.

Record the current `End offset` of every partition. Wait until its `Committed offset` reaches that boundary. For example, `115` has not reached a boundary of `120`, while `120` confirms processing up to it. Check all changefeeds and partitions.

This only checks messages already in the changefeeds. It does not prove that all changes have left internal source queues. Zero lag, completed scanning, or `running` alone cannot establish transfer completeness.

### Compare All Data and Check the Application {#check-data}

The CDC checks above do not establish when the final changes have left internal source queues. For this single-table example, confirm readiness by comparing every key and value. Include the time to read both tables and space for both exports in the downtime plan.

Data in both tables must remain unchanged during verification. Source writes have already stopped; also prevent background changes, including TTL deletion. To keep the target unchanged between comparison and completion, [pause replication](../../yql/reference/syntax/alter-async-replication.md) on the target:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'PAUSED');
```

Wait until the description contains `paused` without `error`:

```bash
{{ ydb-cli }} -p target scheme describe my_replication --format proto-json-base64
```

Check that both table descriptions have the same column names and types and the same primary key columns in the same order. Build a complete column list from the schema, including every primary key column, and pass it through `--columns` in the same order to both exports. Replace `id,value` below with the complete column list for your table. Then [stream-read the full tables](../../reference/ydb-cli/commands/readtable.md), ordered by primary key, and compare the exports:

```bash
columns='id,value'
{{ ydb-cli }} -p source table read original_table --ordered --columns "$columns" --format json-base64 > source.rows.jsonl 2> source.read.stderr &&
{{ ydb-cli }} -p target table read replica_table --ordered --columns "$columns" --format json-base64 > target.rows.jsonl 2> target.read.stderr &&
test ! -s source.read.stderr &&
test ! -s target.read.stderr &&
cmp source.rows.jsonl target.rows.jsonl
```

Use the same CLI version and output format. `--ordered` orders rows; the shared complete `--columns` list orders fields. Do not specify `--limit`, `--from`, or `--to`: verification must include every row and column. The example runs `cmp` only after both commands succeed and both stderr files are empty. This also rejects a handled interruption: the CLI can write `<INTERRUPTED>` to stderr and return exit code `0`. If the chain stops, inspect `source.read.stderr` and `target.read.stderr`, fix the cause, and repeat both full exports. Do not delete messages from the files to continue the chain. Partial files are invalid even if they match. `cmp` must return exit code `0` with no output, indicating byte-for-byte equality of the complete exports. For multiple tables without secondary indexes, repeat the full comparison for every source and target pair.

If the files differ, investigate the differences. If the target has not applied all changes, resume with `STATE = 'STANDBY'`, wait for progress, and repeat pausing and both full exports. Keep source writes stopped throughout. Do not run `DONE/FORCE` until full comparison succeeds and the data remains unchanged. If verification cannot fit the agreed window, stop the procedure and agree on further actions with your installation's support team.

After successful comparison, use application credentials to read the target by known keys. You may additionally compare row counts:

```yql
-- On the source:
SELECT COUNT(*) FROM original_table;

-- On the target:
SELECT COUNT(*) FROM replica_table;
```

On large tables, `COUNT(*)` can take a long time. Equal counts and sampled queries do not replace comparing every key and value.

Proceed to [completion and cutover](#cutover) only after full comparison and application checks succeed. Keep replication `PAUSED` and both tables unchanged until `DONE/FORCE`; do not resume applying changes between comparison and completion.

## 5. Complete Replication and Switch the Application {#cutover}

{% note warning %}

`DONE/FORCE` does not check completeness or lag. A completed replication instance cannot resume. Run it only after [readiness verification](#readiness).

{% endnote %}

Run [ALTER ASYNC REPLICATION](../../yql/reference/syntax/alter-async-replication.md) **on the target**:

```yql
ALTER ASYNC REPLICATION my_replication
SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');
```

```bash
{{ ydb-cli }} -p target scheme describe my_replication --format proto-json-base64
```

Wait until `done` appears; neither `paused` nor `running` should remain. Using application credentials, perform an agreed test write, read it, and remove the test data on the target. Verify that all required tables are writable.

Switch application endpoints, database paths, TLS/CA settings, and authentication to the target. In this example, the source table is `original_table` and the target is `replica_table`: update the name in relative application SQL queries as well. Also check absolute SQL paths, `PRAGMA TablePathPrefix`, migration tools, and background jobs. Prepare missing objects and indexes before running dependent queries, then recreate connections using the application's deployment procedure.

Enable writes **only on the target** and verify real application scenarios. **Write downtime ends here.** Keep source writes disabled. After new target writes, returning to the source requires reconciling new changes; changing the endpoint back is insufficient.

## 6. Drop the Replication Instance, If Needed {#cleanup}

Deletion is optional after cutover. Run it separately after verifying the target data and confirming that this replication's source changefeeds are no longer needed.

| Goal | Action |
| --- | --- |
| Keep target tables writable | First complete replication through `DONE` and test writes |
| Remove replication but preserve target tables | Run ordinary `DROP ASYNC REPLICATION` after completion; source changefeeds are also removed |
| Remove replication and target tables | Use `CASCADE` only when intentionally deleting target data |

Run [DROP ASYNC REPLICATION](../../yql/reference/syntax/drop-async-replication.md) **on the target**:

```yql
DROP ASYNC REPLICATION my_replication;
```

Keep source permissions until cleanup finishes. Verify removal of the instance and its changefeeds, preservation of target tables, and removal of source schema restrictions. Remove secrets and access only after checking that other users no longer need them.

If the source is unavailable or deletion stalls, contact your installation's support team. Do not retry with `CASCADE`. Provide database paths, replication name, server and CLI versions, time and error text, instance state, and last known positions. Do not include credentials.

## If the Source Is Unavailable {#source-unavailable}

For a temporary failure, preserve the ability to resume replication and monitor CDC history availability. Do not complete or recreate replication to fix a connection error.

If the source cannot be restored in time, the application owner must decide what data loss is acceptable for an emergency cutover. Record available tables, last known applied positions, and checks that cannot be performed. Prevent source writers from resuming when it recovers.

The target state alone cannot prove that all latest source writes arrived. `DONE/FORCE` cannot recover undelivered changes. Before completion, agree with support on whether the transition is possible for the current version and state. Afterward, verify writes and application behavior as for a planned cutover.

## See Also {#see-also}

- [Asynchronous replication](../../concepts/async-replication.md).
- [Choosing a migration method](index.md).

# Choosing a Cross-Cluster Migration Method

## Introduction {#vvedenie}

When migrating, you transfer data to a new database and switch the application to it. Choose a method based on acceptable write downtime, data volume, and available resources. Prepare a plan and validate it in a test migration.

## Migration Plan {#plan}

Before starting the migration:

1. Record the source and target databases, tables and other objects, users, and applications.
1. Define acceptable read and write downtime. Assign owners for stopping writes, transferring and verifying data, switching the application, and deciding whether to return to the source. Record who confirms completion of each stage before the next one begins.
1. Prepare the target database, application permissions, network access, and resources for both databases.
1. Temporarily stop writes in a test application. Check that the client handles write unavailability within the agreed timeout, does not treat failed operations as successful, and does not create duplicates through retries. After restoring access and recreating the connection, verify a successful write and read its result.
1. Choose verification queries, conditions for starting cutover, and actions on failure. Include the time for [full data comparison](async-replication.md#check-data) in the downtime window and reserve space for the exports. Run a test migration and test returning to the source before new writes appear on the target.
1. Agree on a migration window and notify users.

## Choosing a Migration Method {#approach-to-downtime}

| Migration method | Write downtime | Preparation |
| --- | --- | --- |
| Backup and restore | From stopping writes before backup creation to enabling writes after application cutover | A [backup and restore procedure](../backup-and-recovery/index.md) and new application connection settings |
| Asynchronous replication | Final synchronization, data verification, and cutover | A [replication and migration procedure](async-replication.md), space for accumulated changefeed messages, and new application connection settings |

Backup and restore is suitable when writes can stop before backup creation and resume only after restoring data and switching the application. Replication transfers most data while the source remains in use, but requires monitoring changes and a separate completion step before writes can start on the target. {#async-replication}

## Standalone Migration Using Backup and Restore {#backup-restore}

Use this procedure without a replication instance or changefeeds when the write downtime described above is acceptable:

1. Choose a [backup and restore method](../backup-and-recovery/index.md). Prepare backup storage, access permissions, and network connectivity for the chosen tools. Ensure that destination paths on the target database are unused. Record the full object set, application permissions, data checks, and downtime window. Save the source endpoint and database path from connection settings, the backup command, object list, and intended backup location.
1. **Start of write downtime.** Stop all applications and jobs writing to the source, wait for active queries and transactions to finish, and prevent writes from restarting automatically.
1. Create a new backup of the selected objects and wait for successful completion. In the saved command and launch log, compare the endpoint and database path with the planned source, and the object list and backup location with those recorded in the first step. If the tool uses a connection profile, check the endpoint and database path in that profile. For a background operation, also save its identifier and [check its status](../../reference/ydb-cli/operation-get.md). Do not use a backup whose source and contents cannot be confirmed.
1. Restore the backup into the target database. Wait for successful completion and verify the objects and application permissions. Preserve object names unless you planned to change them.
1. Run the agreed data checks and read using application credentials. On errors, do not switch the application or enable target writes.
1. Update the endpoint, database path, TLS settings and certificate authority (CA) certificate, authentication, and object paths in all clients and background jobs. Recreate connections, verify reads, and perform an agreed test write. After successful checks, enable production writes **only on the target**. **End of write downtime.** Keep source writes disabled.

After new target writes, switching back to the source alone is insufficient: returning requires reconciling and transferring the new changes.

## Choosing Initial Synchronization {#initial-sync}

During migration, asynchronous replication first transfers existing data from the source to the target, then keeps it up to date by transferring changes. Initial synchronization creates the starting dataset to which these changes will be applied.

Choose one initial synchronization method **before creating replication**:

- **[Initial scan](../../concepts/async-replication.md#initial-scan)** does not require separate S3 storage. Initial data and subsequent changes pass through changefeeds.
- **[Backup and restore](../async-replication-initial-sync.md)** transfers initial data through S3; changefeeds carry changes accumulated during the copy. S3 must be accessible to the servers of both databases, and changefeeds must be ready before backup creation starts.

For tables larger than 1 TB, first test backup and restore in a trial transfer. Compare its duration and load on both databases with an initial scan. Choose a method that transfers initial data and accumulated changes within the planned time without exceeding available resources. Initial scanning is also supported for tables larger than 1 TB; size alone does not determine the choice.

When comparing methods, account for data volume, application write rate, disk and network throughput, CPU and memory headroom, backup creation time (export to S3), restoration time (import from S3), and the [availability of accumulated changes](../../concepts/async-replication.md#availability-period).

## Grouping Tables into Replication Instances {#replication-scope}

First determine the application's [data consistency requirements](../../concepts/async-replication.md#consistency-levels). If reads on the target require atomic visibility of transactions that change related tables, include those tables in one instance with `CONSISTENCY_LEVEL = 'GLOBAL'`. Separate instances can apply changes at different times and do not provide this guarantee across instances.

When independent application of changes is acceptable, tables from different applications and large tables of one application can use separate instances. List the tables of one instance together in its `FOR` clause when [creating replication](async-replication.md#create). Record the selected set before starting.

## Stopping Writes {#stopping-traffic}

For standalone migration using a backup, first stop writes to the source and wait for active transactions to finish. Then create a backup using the [procedure above](#backup-restore).

For replication-based migration, stop writes to the source before final synchronization and application cutover. **Start of write downtime.** Stop writes from all applications, background jobs, and loaders. Stop the processes, switch the application to a read-only mode, or change [permissions](../../security/authorization.md). Wait for active transactions to complete and prevent old application instances from automatically resuming writes.

For checking changes and data, completing replication, and switching the application, see [Migration Using Asynchronous Replication](async-replication.md#readiness). Enable writes only on the target after completing those steps. **End of write downtime.** After new target writes, returning to the source requires accounting for those changes.

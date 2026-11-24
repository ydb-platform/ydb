# Choosing a Cross-Cluster Migration Method

Migration includes transferring data to a new database and switching the application. Choose a method based on acceptable write downtime, data volume, and available resources. Prepare a plan and validate it in a test migration.

## Migration Plan {#plan}

Before the transfer:

1. Record the source and target databases, tables and other objects, users, and applications.
1. Define acceptable read and write downtime and assign the people responsible.
1. Prepare the target, application permissions, network access, and resources for both databases.
1. Test application reconnection and retries during temporary write unavailability.
1. Choose verification queries, conditions for starting cutover, and actions on failure. Run a test migration and test returning to the source before new writes appear on the target.
1. Agree on a migration window and notify users.

## Choosing a Method {#approach-to-downtime}

| Method | Write downtime | Preparation |
| --- | --- | --- |
| Backup and restore | Backup creation, restoration, and application cutover | A [backup and restore procedure](../backup-and-recovery/index.md) and new application connection settings |
| Asynchronous replication | Final synchronization, data verification, and cutover | A [replication and migration procedure](async-replication.md), space for accumulated CDC, and new application connection settings |

Backup and restore is suitable when writes can stop for the entire transfer. Replication transfers most data while the source remains in use, but requires monitoring changes and a separate completion step before writes can start on the target.

## Standalone Migration Using Backup and Restore {#backup-restore}

Use this procedure without a replication instance or CDC when writes can stop for the entire transfer:

1. Prepare the target, [backup and restore operations](../backup-and-recovery/index.md), application permissions, the full set of objects to transfer, and data checks. Agree on the downtime window.
1. Stop all source writers, wait for active queries and transactions to finish, and prevent writers from restarting automatically. **Write downtime begins before backup creation.**
1. Create a new backup of the selected objects after writes have stopped. Wait for successful completion and verify that it belongs to the source.
1. Restore the backup into unused paths on the prepared target. Wait for successful completion and verify the objects and application permissions. Preserve object names unless you planned to change them. This procedure does not require creating replication, starting it with `STANDBY`, or completing it with `DONE/FORCE`.
1. Run the agreed data checks and read using application credentials. On errors, do not switch the application or enable target writes.
1. Update the endpoint, database path, TLS/CA, authentication, and object paths in all clients and background jobs. Recreate connections, verify reads, and perform an agreed test write. After successful checks, enable production writes **only on the target**. **Write downtime ends here.** Keep source writes disabled.

After new target writes, switching back to the source alone is insufficient: returning requires reconciling and transferring the new changes.

## Choosing Initial Synchronization {#initial-sync}

For replication-based migration, choose one method **before creating replication**:

- **An initial CDC scan** does not require separate S3 storage. Initial data and subsequent changes pass through CDC changefeeds.
- **[Backup and restore](../async-replication-initial-sync.md)** transfers initial data through S3; CDC carries changes accumulated during the copy. S3 must be accessible to the servers of both databases, and changefeeds must be ready before export starts.

For tables larger than 1 TB, consider backup and restore as the initial option. This is an operational guideline rather than a limit on initial scanning: validate the choice by comparing load and duration in a test transfer on your installation.

Compare duration and load in a test transfer. Account for data volume, application write rate, disk and network throughput, CPU and memory headroom, export and import duration, and the [availability of accumulated changes](../../concepts/async-replication.md#availability-period). Table size alone does not determine transfer duration.

## Grouping Tables into Replication Instances {#replication-scope}

Use separate replication instances for tables belonging to different applications. Consider transferring large tables of one application separately as well. List tables transferred together in one `FOR` clause when [creating replication](async-replication.md#create). Record the selected set before starting, and separately check the application's data consistency requirements.

## Stopping Writes {#stopping-traffic}

For a standalone backup migration, stop writes [before creating the backup](#backup-restore). For replication-based migration, before final cutover stop writes to the source from all applications, background jobs, and loaders. Stop the processes, switch the application to a read-only mode, or change [permissions](../../security/authorization.md). Wait for active transactions to complete and prevent old application instances from automatically resuming writes.

For checking changes and data, completing replication, and switching the application, see the [step-by-step procedure](async-replication.md#readiness). After writes start on the target, returning to the source requires accounting for new changes.

# Backup and Recovery

{{ ydb-short-name }} is designed from the ground up to maximize data safety against hardware failures. {{ ydb-short-name }} offers redundancy options across different numbers of availability zones, racks, hosts, disks, and more. This significantly reduces the chances of data loss due to hardware failures. However, keeping an additional copy of your data on a separate medium or in cloud S3 storage is still recommended.

Data loss can occur not just physically, but also logically. For example, in the event of a system breach, attackers may gain access to the database and intentionally corrupt data using legitimate operations from the DB perspective (such as delete or update commands). Data can also be corrupted as a result of application software errors, where part of the data is updated by an incorrect algorithm, potentially resulting in significant business losses and operational problems. In all these cases, having reliable backups is essential for data recovery and business continuity.

{{ ydb-short-name }} offers multiple approaches for creating backups, which are outlined below.

## Backup Methods {#by-storage}

#|
|| **Backup method** | **Storage type** | **Backup type** | **Survives cluster failure** | **Use cases** ||
|| [{{ ydb-short-name }} CLI Dump/Restore](#files) | File system | Full backups | Yes | Local development, testing, smaller production environments ||
|| [{{ ydb-short-name }} CLI Import/Export S3](#s3) | S3-compatible storage | Full backups | Yes | Large data migration, archival, production data transfers ||
|| [Backup collections](#backup-collections) | Cluster storage | Full and incremental backups | No | Production environments with regular backup schedules, large datasets ||
|#

{% note info %}

Cluster-stored backups benefit from {{ ydb-short-name }}'s built-in replication and fault tolerance but do not protect against cluster-wide failures. For disaster recovery, combine cluster-stored backup collections with regular exports to external storage.

{% endnote %}

## {{ ydb-short-name }} CLI Dump/Restore {#files}

The following commands are used to back up files:

- `{{ ydb-cli }} admin cluster dump` — for backing up cluster metadata
- `{{ ydb-cli }} admin database dump` — for backing up a database
- `{{ ydb-cli }} tools dump` — for backing up individual schema objects or directories

You can learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-dump.md).

The following commands are used to perform recovery from a file backup:

- `{{ ydb-cli }} admin cluster restore` — for restoring cluster metadata from a backup
- `{{ ydb-cli }} admin database restore` — for restoring a database from a backup
- `{{ ydb-cli }} tools restore` — for restoring individual schema objects or directories from a backup

You can learn more about these commands in [{#T}](../../reference/ydb-cli/export-import/tools-restore.md).

## {{ ydb-short-name }} CLI Import/Export S3 {#s3}

The `{{ ydb-cli }} export s3` command is used to back up data to S3-compatible storage (for example, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)). Follow [this link](../../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

The `{{ ydb-cli }} import s3` command is used to recover data from a backup created in S3-compatible storage. Follow [this link](../../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

{% note info %}

The speed of backup and recovery operations to/from S3-compatible storage is configured to minimize impact on user workload. To control the speed of operations, configure limits for the corresponding queue in the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

{% note info %}

When running the export operation, a directory named `export_*` is created in the root directory, where `*` is the numeric part of the export ID. This directory stores tables with a consistent snapshot of exported data as of the export start time. After a successful backup, the `export_*` directory and its contents are removed.

{% endnote %}

## Backup Collections {#backup-collections}

Backup collections allow the use of the `BACKUP` and `RESTORE` commands to perform full and incremental backups for production workloads. For more information on the concepts and architecture, see [Backup collections](../../concepts/datamodel/backup-collection.md) and for details about full and incremental backup methods, refer to the [Full and Incremental Backups](./full-and-incremental-backups.md) section.

Backup collections are best suited for production environments that require scheduled backups, large datasets where incremental backups minimize data transfer, and point-in-time recovery needs.

For simpler scenarios, such as one-time migrations, development environments, or small databases, consider using [export/import](#s3) or [dump/restore](#files) commands instead.

{% cut "is it needed?" %}

### Retention and Cleanup

{% note warning %}

Before deleting backups, understand chain dependencies:

- **Full backups** are required for all subsequent incrementals
- **Incremental backups** depend on the full backup and all preceding incrementals
- Deleting any backup in a chain makes subsequent incrementals unrestorable

YDB does not provide built-in chain integrity verification. Manually track which backups belong to which chain.

{% endnote %}

#### Safe cleanup approach {#safe-cleanup}

1. Create a new full backup
2. Verify the new backup is complete
3. Export old backup chains to external storage if needed
4. Delete old backup chains (full backup + all its incrementals together)

```bash
# Remove old backup chain
ydb scheme rmdir -r .backups/collections/production_backups/20250208141425Z_full/
ydb scheme rmdir -r .backups/collections/production_backups/20250209141519Z_incremental/
```

### Best Practices

For detailed guidance on backup collection best practices, see the [Backup collection recipes](../../recipes/backup-collections/index.md):

- [Exporting to external storage](../../recipes/backup-collections/exporting-to-external-storage.md) — disaster recovery with S3 or filesystem
- [Maintenance and cleanup](../../recipes/backup-collections/maintenance-and-cleanup.md) — managing storage and backup chains
- [Validation and testing](../../recipes/backup-collections/validation-and-testing.md) — verifying backup integrity

{% endcut %}
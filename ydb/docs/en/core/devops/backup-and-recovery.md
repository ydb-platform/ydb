# Backup and Recovery

Backup is used to protect against data loss, allowing you to restore data from a backup copy.

{{ ydb-short-name }} provides several solutions for performing backup and recovery. For conceptual information and comparison of backup methods, see [Backup concepts](../concepts/backup.md).

{% include [_includes/backup_and_recovery/options_overlay.md](_includes/backup_and_recovery/options_overlay.md) %}

## {{ ydb-short-name }} CLI {#cli}

### Files {#files}

The following commands are used to back up files:

- `{{ ydb-cli }} admin cluster dump` — for backing up cluster metadata
- `{{ ydb-cli }} admin database dump` — for backing up a database
- `{{ ydb-cli }} tools dump` — for backing up individual schema objects or directories

You can learn more about these commands in [{#T}](../reference/ydb-cli/export-import/tools-dump.md).

The following commands are used to perform recovery from a file backup:

- `{{ ydb-cli }} admin cluster restore` — for restoring cluster metadata from a backup
- `{{ ydb-cli }} admin database restore` — for restoring a database from a backup
- `{{ ydb-cli }} tools restore` — for restoring individual schema objects or directories from a backup

You can learn more about these commands in [{#T}](../reference/ydb-cli/export-import/tools-restore.md).

### S3-Compatible Storage {#s3}

The `{{ ydb-cli }} export s3` command is used to back up data to S3-compatible storage (for example, [AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html)). Follow [this link](../reference/ydb-cli/export-import/export-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

The `{{ ydb-cli }} import s3` command is used to recover data from a backup created in S3-compatible storage. Follow [this link](../reference/ydb-cli/export-import/import-s3.md) to the {{ ydb-short-name }} CLI reference for information about this command.

{% note info %}

The speed of backup and recovery operations to/from S3-compatible storage is configured to minimize impact on user workload. To control the speed of operations, configure limits for the corresponding queue in the [resource broker](../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

{% note info %}

When running the export operation, a directory named `export_*` is created in the root directory, where `*` is the numeric part of the export ID. This directory stores tables with a consistent snapshot of exported data as of the export start time. After a successful backup, the `export_*` directory and its contents are removed.

{% endnote %}

## Backup Collections {#backup-collections}

Backup collections enable incremental backups and point-in-time recovery for production workloads. For conceptual information and architecture details, see [Backup collections](../concepts/datamodel/backup-collection.md).

### When to Use Backup Collections

Backup collections are recommended for:

- **Production environments** requiring regular backup schedules
- **Large datasets** where incremental changes are much smaller than total data size
- **Point-in-time recovery** requirements

For simpler scenarios (one-time migrations, development environments, small databases), consider using [export/import](#s3) or [dump/restore](#files) instead.

### Setting Up Production Backups

A typical production backup setup involves:

1. **Plan your backup strategy**: Determine backup frequency (daily full, hourly incremental), retention period, and external storage requirements.

2. **Create a backup collection**: Define which tables to include using the `CREATE BACKUP COLLECTION` SQL statement.

3. **Schedule backups externally**: {{ ydb-short-name }} does not provide built-in scheduling. Use cron or similar tools to execute backup commands.

4. **Configure external storage exports**: For disaster recovery, regularly export backup collections to S3 or filesystem storage.

5. **Monitor and maintain**: Track backup operations, verify chain integrity, and manage retention.

### Creating Backup Collections

Create a collection using SQL:

```sql
CREATE BACKUP COLLECTION `production_backups`
    ( TABLE `/Root/mydb/orders`, TABLE `/Root/mydb/customers` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Taking Backups

**Full backup** (required first, then periodically):

```sql
BACKUP `production_backups`;
```

Use the `BACKUP` command to trigger backup operations.

**Incremental backup** (after initial full backup):

```sql
BACKUP `production_backups` INCREMENTAL;
```

**Example schedule** (implemented via external scheduler):

- Sunday 2:00 AM: Full backup
- Monday-Saturday 2:00 AM: Incremental backup

### Monitoring Operations

Backup operations run asynchronously. Monitor progress using:

```bash
# List backup operations
ydb operation list incbackup

# Check specific operation status
ydb operation get <operation-id>
```

Browse backup structure:

```bash
# List all collections
ydb scheme ls .backups/collections/

# View backups in a collection
ydb scheme ls .backups/collections/production_backups/
```

{% note warning %}

Canceling backup operations is not yet supported. The `ydb operation cancel` command will return an error for backup operations.

{% endnote %}

### Restoring from Backups

To restore data, use the `RESTORE` command:

```sql
RESTORE `production_backups`;
```

**Disaster recovery from external storage:**

If you've exported backups to external storage, import them first:

```bash
# Import from filesystem
ydb tools restore -i /path/to/backup -p .backups/collections/production_backups

# Or import from S3
ydb import s3 --s3-endpoint storage.example.com \
  --bucket my-backups \
  --item src=production_backups,dst=.backups/collections/production_backups
```

Then execute the RESTORE command.

### Retention and Cleanup

{% note warning %}

Before deleting backups, understand chain dependencies:

- **Full backups** are required for all subsequent incrementals
- **Incremental backups** depend on the full backup and all preceding incrementals
- Deleting any backup in a chain makes subsequent incrementals unrestorable

YDB does not provide built-in chain integrity verification. Manually track which backups belong to which chain.

{% endnote %}

**Safe cleanup approach:**

1. Create a new full backup
2. Verify the new backup is complete
3. Export old backup chains to external storage if needed
4. Delete old backup chains (full backup + all its incrementals together)

```bash
# Remove old backup chain
ydb scheme rmdir -r .backups/collections/production_backups/20250208141425Z_full/
ydb scheme rmdir -r .backups/collections/production_backups/20250209141519Z_incremental/
```

### Exporting to External Storage

For disaster recovery, export backup collections to external storage:

**To S3-compatible storage:**

```bash
ydb export s3 --s3-endpoint storage.example.com \
  --bucket my-backups \
  --item src=.backups/collections/production_backups,dst=production_backups
```

**To filesystem:**

```bash
ydb tools dump -p .backups/collections/production_backups \
  -o /path/to/backup/production_backups
```

### Best Practices

For detailed guidance on backup collection best practices, see the [Backup collection recipes](../recipes/backup-collections/index.md):

- [Exporting to external storage](../recipes/backup-collections/exporting-to-external-storage.md) — disaster recovery with S3 or filesystem
- [Maintenance and cleanup](../recipes/backup-collections/maintenance-and-cleanup.md) — managing storage and backup chains
- [Validation and testing](../recipes/backup-collections/validation-and-testing.md) — verifying backup integrity

{% include [_includes/backup_and_recovery/cli_overlay.md](_includes/backup_and_recovery/cli_overlay.md) %}

{% include [_includes/backup_and_recovery/others_overlay.md](_includes/backup_and_recovery/others_overlay.md) %}

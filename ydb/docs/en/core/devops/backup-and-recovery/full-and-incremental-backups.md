# Full and Incremental Backups

{% include [feature_enterprise.md](../../_includes/feature_enterprise.md) %}

{% note warning %}

This functionality is under active development. Please be aware of existing [limitations](../../concepts/datamodel/backup-collection.md#limitations) and consult the documentation for the most up-to-date information.

{% endnote %}

A typical production backup setup involves:

1. **Plan your backup strategy**: Determine backup frequency (daily full, hourly incremental), retention period, and external storage requirements.

2. **Create a backup collection**: Define which tables to include using the [`CREATE BACKUP COLLECTION`](../../yql/reference/syntax/create-backup-collection.md) SQL statement.

3. **Schedule backups externally**: {{ ydb-short-name }} does not provide built-in scheduling. Use cron or similar tools to execute backup commands.

4. **Configure external storage exports**: For disaster recovery, regularly export backup collections to S3 or filesystem storage.

5. **Monitor and maintain**: Track backup operations, verify chain integrity, and manage retention.

## Creating Backup Collections

Create a collection using YQL:

```sql
CREATE BACKUP COLLECTION `production_backups`
    ( TABLE `/Root/mydb/orders`, TABLE `/Root/mydb/customers` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

## Full Backups

A full backup creates a complete snapshot of all data in your backup collection at a specific point in time. Full backups are self-contained and can be restored independently without requiring any previous backups.

### When to use full backups

- **Initial backup**: You must create a full backup before you can use incremental backups. The first backup in any collection is always a full backup.
- **Periodic full backups**: Even when using incremental backups, schedule periodic full backups (e.g., weekly or monthly) to reduce dependency chains and simplify recovery scenarios.
- **Small datasets**: For small databases where the overhead of managing incremental backup chains outweighs the benefits.

### Creating a full backup

1. Ensure you have created a backup collection (see [Creating Backup Collections](#creating-backup-collections)).

2. Execute the backup command:

```sql
BACKUP `production_backups`;
```

This command creates a full backup of all tables in the specified collection. You can track the progress of the full backup by monitoring the progress of the corresponding SQL operation. Once the SQL operation completes, the backup is finished.

### Restoring From Backup

To restore data from a backup collection, use the [`RESTORE`](../../yql/reference/syntax/restore-backup-collection.md) command in YQL. Restoration can be performed from any full backup or from an incremental backup chain (full + incrementals).

#### Restoring the latest backup

```sql
RESTORE `production_backups`;
```

By default, this restores the most recent backup in the collection. The restore operation restores the data to exactly the same locations (paths and table names) from which the backup was originally created. For the restore operation to succeed, there must not be any existing tables at the target paths that are being restored.

## Incremental Backups

If your tables grows too large for daily full backups, you can take less frequent full backups (e.g., weekly) with daily incremental backups. Incremental backups are storage efficient and faster than full backups for larger tables.

An incremental backup captures only the changes (inserts, updates, deletes) that have occurred since the last backup in the collection. Incremental backups are smaller and faster than full backups, but they depend on previous backups in the chain and cannot be restored independently.

### When to use incremental backups

- **Regular backups**: Use incremental backups for frequent backups (e.g., hourly or daily) to minimize storage usage and backup time while maintaining recent recovery points.
- **Large datasets**: For large databases where full backups are time-consuming or resource-intensive, incremental backups provide a more efficient backup strategy.
- **Production environments**: In production systems with continuous data changes, incremental backups allow you to maintain multiple recovery points without the overhead of full backups.

{% note info "Prerequisites" %}

Before creating incremental backups, you must have at least one full backup in the collection. The first backup in any collection is always a full backup.

{% endnote %}

### Creating an incremental backup

1. Ensure you have created a backup collection with incremental backups enabled (see [Creating Backup Collections](#creating-backup-collections)).

2. Ensure you have at least one full backup in the collection. If this is your first backup, create a full backup first (see [Creating a full backup](#creating-a-full-backup)).

3. Execute the incremental backup command:

```sql
BACKUP `production_backups` INCREMENTAL;
```

This command creates an incremental backup containing only the changes since the last backup in the collection. The backup operation runs asynchronously and does not block normal database operations.

### Restoring From Backup

To restore data from a backup collection, use the [`RESTORE`](../../yql/reference/syntax/restore-backup-collection.md) command in YQL. Restoration can be performed from any full backup or from an incremental backup chain (full + incrementals).

#### Restoring the latest backup

```sql
RESTORE `production_backups`;
```

By default, this restores the most recent backup in the collection.

## Exporting Backups to External Storage {#s3export}

{% note warning %}

By default, backups created with backup collections are stored in the cluster's internal storage. While this ensures protection against disk or node failures within the cluster, these backups remain in the same fault domain as your primary data. In the case of catastrophic failure or total cluster loss, both your production data and internal backups may become unavailable.

**To safeguard your backups against such scenarios, it is recommended to regularly export vital backups to external storage, such as S3-compatible object storage.** This provides an additional layer of protection and enables disaster recovery even if the entire cluster is lost.

{% endnote %}

To export a backup from a collection to S3, use the `ydb export s3` command. Refer to the [Export to S3 documentation](../../reference/ydb-cli/export-import/export-s3.md) for syntax and options.

Exporting all backups in a chain independently and preserving their order is important for a successful restore in the future.

## Monitoring Operations {#incbackup-monitoring}

Incremental backup operations run asynchronously. Monitor progress using:

### List backup operations

```bash
ydb operation list incbackup
┌───────────────────────────────────────┬───────┬─────────┬──────────┐
│ id                                    │ ready │ status  │ progress │
├───────────────────────────────────────┼───────┼─────────┼──────────┤
│ ydb://incbackup/11?id=562949953466346 │ true  │ SUCCESS │ Done     │
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┤
│ Created by: root                                                   │
│ Create time: 2025-12-26T13:05:24Z                                  │
│ End time: 2025-12-26T13:05:25Z                                     │
└────────────────────────────────────────────────────────────────────┘
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

## Retention and Cleanup

{% note warning %}

Before deleting backups, understand chain dependencies:

- **Full backups** are required for all subsequent incrementals
- **Incremental backups** depend on the full backup and all preceding incrementals
- Deleting any backup in a chain makes subsequent incrementals unrestorable

{{ ydb-short-name }} does not provide built-in chain integrity verification. Manually track which backups belong to which chain.

{% endnote %}

### Safe cleanup approach {#safe-cleanup}

1. Create a new full backup
2. Verify the new backup is complete
3. Export old backup chains to external storage if needed
4. Delete old backup chains (full backup + all its incrementals together) by [CLI](../../reference/ydb-cli/commands/dir.md#rmdir)

```bash
ydb scheme rmdir -r .backups/collections/production_backups/path_to_old_full_backup
ydb scheme rmdir -r .backups/collections/production_backups/path_to_old_incremental_backup
```

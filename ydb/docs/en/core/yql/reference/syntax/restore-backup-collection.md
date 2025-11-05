# RESTORE

The `RESTORE` statement restores tables from a [backup collection](../../../concepts/backup-collections.md).

```yql
RESTORE `collection_name`;
```

## Parameters

* `collection_name`: Name of the backup collection to restore from.

## Restore behavior

The restore operation:

* Restores all tables from the most recent backup in the collection
* If incremental backups exist, applies them on top of the last full backup
* Restores tables to their state at the time of the backup
* Overwrites existing tables with the same names

{% note warning %}

The restore operation will overwrite any existing tables with the same names. Make sure to back up or rename existing tables before restoring if you want to preserve them.

{% endnote %}

## Examples

Restoring from a backup collection:

```yql
-- Restore all tables from the collection
RESTORE `production_backups`;
```

## Monitoring restore operations

Restore operations run asynchronously in the background. You can monitor their progress using YDB CLI:

```bash
# List restore operations
ydb operation list incbackup

# Get operation details
ydb operation get <operation-id>
```

## See also

* [Backup collections concepts](../../../concepts/backup-collections.md)
* [CREATE BACKUP COLLECTION](create-backup-collection.md)
* [BACKUP](backup.md)
* [DROP BACKUP COLLECTION](drop-backup-collection.md)
* [Operations guide](../../../maintenance/manual/backup-collections.md)

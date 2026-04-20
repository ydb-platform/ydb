# RESTORE

The `RESTORE` statement restores tables from a [backup collection](../../../concepts/datamodel/backup-collection.md).

```yql
RESTORE collection_name;
```

## Parameters

* `collection_name` — name of the backup collection to restore from.

## Restore behavior

The restore operation:

* Finds the **current chain** of backups in the collection (the latest full backup followed by its incremental backups).
* Restores tables to the state of the **most recent** backup in that chain: applies the full backup and all subsequent incrementals in sequence.
* Fails if any of the tables being restored already exists at the same path.

Restoring to an arbitrary point "between" two saved backups is not supported: the result is always the state captured by one of the backups in the chain.

{% note warning %}

The restore operation fails if any of the tables being restored already exists at the same path. Rename or drop the conflicting tables before restoring.

{% endnote %}

## Examples

Restoring from a backup collection:

```yql
-- Restore all tables from the collection
RESTORE production_backups;
```

## Monitoring restore operations

Restore operations run asynchronously in the background. You can monitor their progress using {{ ydb-short-name }} CLI:

```bash
# List backup and restore operations
ydb operation list incbackup

# Get operation details
ydb operation get <operation-id>
```

## See also

* [Backup collections](../../../concepts/datamodel/backup-collection.md)
* [CREATE BACKUP COLLECTION](create-backup-collection.md)
* [BACKUP](backup.md)
* [DROP BACKUP COLLECTION](drop-backup-collection.md)
* [Backup and recovery guide](../../../devops/backup-and-recovery.md)

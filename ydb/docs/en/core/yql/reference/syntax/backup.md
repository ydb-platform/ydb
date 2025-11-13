# BACKUP

The `BACKUP` statement creates a backup of tables in a [backup collection](../../../concepts/backup-collections.md).

```yql
BACKUP collection_name [INCREMENTAL];
```

## Parameters

* `collection_name` — name of the backup collection.
* `INCREMENTAL` — create an incremental backup instead of a full backup.

## Backup types

### Full backup

A full backup creates a snapshot of all tables in the collection at a specific point in time. This serves as the baseline for subsequent incremental backups.

```yql
BACKUP production_backups;
```

### Incremental backup

An incremental backup captures only the changes (inserts, updates, deletes) since the previous backup in the chain. The collection must have been created with `INCREMENTAL_BACKUP_ENABLED = 'true'`.

```yql
BACKUP production_backups INCREMENTAL;
```

{% note warning %}

Incremental backups require a previous full backup in the same collection. Always create a full backup first before taking incremental backups.

{% endnote %}

## Examples

Creating an initial full backup:

```yql
-- First, create a collection
CREATE BACKUP COLLECTION daily_backups (
    TABLE orders
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);

-- Then create the full backup
BACKUP daily_backups;
```

Creating incremental backups:

```yql
-- After the initial full backup, create incremental backups
BACKUP daily_backups INCREMENTAL;
```

## Monitoring backup operations

Backup operations run asynchronously in the background. You can monitor their progress using {{ ydb-short-name }} CLI:

```bash
# List backup operations
ydb operation list incbackup

# Get operation details
ydb operation get <operation-id>
```

## See also

* [Backup collections](../../../concepts/backup-collections.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).
* [Backup collections operations guide](../../../maintenance/manual/backup-collections.md).

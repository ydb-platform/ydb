# DROP BACKUP COLLECTION

The `DROP BACKUP COLLECTION` statement deletes a [backup collection](../../../concepts/backup-collections.md) and all its backups.

```yql
DROP BACKUP COLLECTION collection_name;
```

## Parameters

* `collection_name` — name of the backup collection to drop.

{% note warning %}

This operation permanently deletes the backup collection and all backups it contains. This action cannot be undone.

{% endnote %}

## Examples

Dropping a backup collection:

```yql
DROP BACKUP COLLECTION old_backups;
```

## See also

* [Backup collections](../../../concepts/backup-collections.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).

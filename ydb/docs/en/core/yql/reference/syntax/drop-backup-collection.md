# DROP BACKUP COLLECTION

The `DROP BACKUP COLLECTION` statement deletes a [backup collection](../../../concepts/datamodel/backup-collection.md) and all its backups.

```yql
DROP BACKUP COLLECTION collection_name;
```

## Parameters

* `collection_name` — name of the backup collection to drop.

{% note warning %}

This operation permanently deletes the backup collection and all backups it contains from the cluster. This action cannot be undone.

{% endnote %}

{% note info %}

Dropping a backup collection only affects cluster-stored data. Any backups previously exported to external storage (S3 or filesystem) are not affected and remain available for import.

{% endnote %}

## Examples

Dropping a backup collection:

```yql
DROP BACKUP COLLECTION old_backups;
```

## See also

* [Backup collections](../../../concepts/datamodel/backup-collection.md).
* [CREATE BACKUP COLLECTION](create-backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).

# CREATE BACKUP COLLECTION

The `CREATE BACKUP COLLECTION` statement creates a [backup collection](../../../concepts/backup-collections.md) - a named set of coordinated backups for selected database objects.

```yql
CREATE BACKUP COLLECTION `collection_name` (
    TABLE `table_path` [, TABLE `table_path` ...]
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## Parameters

* `collection_name`: Name of the backup collection to create.
* `table_path`: Full path to a table to include in the collection. Multiple tables can be specified.
* `STORAGE`: Storage backend for backups. Currently only `'cluster'` is supported.
* `INCREMENTAL_BACKUP_ENABLED`: Enable incremental backup support. Set to `'true'` to enable incremental backups, `'false'` for full backups only.

{% note info %}

When choosing a name for the backup collection, please consider the common [schema objects naming rules](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules)

{% endnote %}

## Examples

Creating a backup collection with a single table:

```yql
CREATE BACKUP COLLECTION `daily_backups` (
    TABLE `/Root/shop/orders`
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

Creating a backup collection with multiple tables:

```yql
CREATE BACKUP COLLECTION `production_backups` (
    TABLE `/Root/shop/orders`,
    TABLE `/Root/shop/products`,
    TABLE `/Root/shop/customers`
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## See also

* [Backup collections concepts](../../../concepts/backup-collections.md)
* [BACKUP](backup.md)
* [RESTORE](restore-backup-collection.md)
* [DROP BACKUP COLLECTION](drop-backup-collection.md)

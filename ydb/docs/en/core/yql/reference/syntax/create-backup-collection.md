# CREATE BACKUP COLLECTION

The `CREATE BACKUP COLLECTION` statement creates a [backup collection](../../../concepts/backup-collections.md).

```yql
CREATE BACKUP COLLECTION collection_name (
    TABLE table_name [, TABLE another_table_name ...]
) WITH (option = value [, ...]);
```

## Parameters

* `collection_name` — name of the backup collection to create.
* `table_name` — full path to a table to include in the collection. Multiple tables can be specified.
* Options:

  * `STORAGE` — storage backend for backups. Currently only `'cluster'` is supported.
  * `INCREMENTAL_BACKUP_ENABLED` — enable incremental backup support. Set to `'true'` to enable incremental backups, `'false'` for full backups only.

{% note info %}

When choosing a name for the backup collection, please consider the common [schema objects naming rules](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules)

{% endnote %}

## Examples

Creating a backup collection with a single table:

```yql
CREATE BACKUP COLLECTION daily_backups (
    TABLE orders
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

Creating a backup collection with multiple tables:

```yql
CREATE BACKUP COLLECTION production_backups (
    TABLE orders,
    TABLE products,
    TABLE customers
) WITH (
    STORAGE = 'cluster',
    INCREMENTAL_BACKUP_ENABLED = 'true'
);
```

## See also

* [Backup collections](../../../concepts/backup-collections.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).

# CREATE BACKUP COLLECTION

The `CREATE BACKUP COLLECTION` statement creates a [backup collection](../../../concepts/datamodel/backup-collection.md).

```yql
CREATE BACKUP COLLECTION collection_name (
    TABLE table_name [, TABLE another_table_name ...]
) WITH (option = value [, ...]);
```

{% note warning %}

In YDB 26.2, backup collections are gated by the
`enable_backup_service` feature flag and are disabled by default. If the flag
is disabled, `CREATE BACKUP COLLECTION` fails with `Backup collections are
disabled`. The flag must be enabled in the cluster configuration before using
backup collections.

{% endnote %}

## Parameters

* `collection_name` — name of the backup collection to create. Use a single
  name, such as `daily_backups`, rather than an arbitrary schema path. YDB
  stores the collection under
  `<database>/.backups/collections/<collection_name>` automatically. Refer to
  the collection by the same name in `BACKUP`, `RESTORE`, and
  `DROP BACKUP COLLECTION` statements.
* `table_name` — full path to a table to include in the collection. Multiple tables can be specified.
* Options:

  * `STORAGE` — storage backend for backups. Supported values:
    * `'cluster'` — storage within the {{ ydb-short-name }} cluster.
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

* [Backup collections](../../../concepts/datamodel/backup-collection.md).
* [BACKUP](backup.md).
* [RESTORE](restore-backup-collection.md).
* [DROP BACKUP COLLECTION](drop-backup-collection.md).

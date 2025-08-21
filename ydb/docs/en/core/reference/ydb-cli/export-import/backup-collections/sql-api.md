# SQL API: Backup collections

This section provides guidance for using SQL commands with backup collections. For complete syntax reference, see [Backup collection commands](../../../yql/reference/syntax/backup-collections.md) in the YQL reference.

## Quick reference

The main SQL commands for backup collections are:

- `CREATE BACKUP COLLECTION` - Creates a new backup collection
- `BACKUP` - Creates a backup (full or incremental)  
- `DROP BACKUP COLLECTION` - Removes a collection and all backups

For detailed syntax, parameters, and examples, refer to the [YQL syntax reference](../../../yql/reference/syntax/backup-collections.md).

## Basic workflow

```sql
-- 1. Create collection
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/shop/orders`, TABLE `/Root/shop/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Take initial full backup
BACKUP `shop_backups`;

-- 3. Take incremental backups
BACKUP `shop_backups` INCREMENTAL;
```

## Query backup information

Browse collections through the database schema:

```bash
# List all collections
ydb scheme ls .backups/collections/

# View specific collection structure  
ydb scheme ls .backups/collections/shop_backups/
```

## Next steps

- [Complete YQL syntax reference for backup collections](../../../yql/reference/syntax/backup-collections.md)
- [Learn about backup collection concepts](concepts.md)
- [Explore all operations and management tasks](operations.md)

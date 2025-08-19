# SQL API: Backup collections

This section provides a complete reference for SQL commands used with backup collections.

## CREATE BACKUP COLLECTION

Creates a new backup collection with specified tables and configuration.

### Syntax

```sql
CREATE BACKUP COLLECTION <collection_name>
    ( TABLE <table_path> [, TABLE <table_path>]... )
WITH ( STORAGE = '<storage_backend>'
     [, INCREMENTAL_BACKUP_ENABLED = '<true|false>']
     [, <additional_options>] );
```

### Parameters

- **collection_name**: Unique identifier for the collection (must be quoted with backticks)
- **table_path**: Absolute path to a table to include in the collection
- **STORAGE**: Storage backend type (currently supports 'cluster')
- **INCREMENTAL_BACKUP_ENABLED**: Enable or disable incremental backup support

### Examples

**Basic collection creation:**
```sql
CREATE BACKUP COLLECTION `my_backups`
    ( TABLE `/Root/database/users` )
WITH ( STORAGE = 'cluster' );
```

**Collection with incremental backups (recommended):**
```sql
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/shop/orders`, TABLE `/Root/shop/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

## BACKUP

Creates a backup within an existing collection. The first backup is always a full backup; subsequent backups can be incremental.

### Syntax

```sql
BACKUP <collection_name> [INCREMENTAL];
```

### Parameters

- **collection_name**: Name of the existing backup collection
- **INCREMENTAL**: Optional keyword to create an incremental backup

### Backup types

**Full backup (default for first backup):**
```sql
BACKUP `shop_backups`;
```

**Incremental backup:**
```sql
BACKUP `shop_backups` INCREMENTAL;
```

### Complete workflow example

```sql
-- 1. Create collection
CREATE BACKUP COLLECTION `sales_data`
    ( TABLE `/Root/sales/transactions`, TABLE `/Root/sales/customers` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Take initial full backup
BACKUP `sales_data`;

-- 3. Take incremental backups
BACKUP `sales_data` INCREMENTAL;
```

## DROP BACKUP COLLECTION

Removes a backup collection and all its associated backups.

### Syntax

```sql
DROP BACKUP COLLECTION <collection_name>;
```

{% note warning %}

This operation is irreversible and will delete all backups in the collection. Ensure you have alternative backups before dropping a collection.

{% endnote %}

## Query backup information

Browse collections through the database schema:

```bash
# List all collections
ydb scheme ls .backups/collections/

# View specific collection structure  
ydb scheme ls .backups/collections/shop_backups/
```
## Limitations and considerations

### Current limitations

- **Storage backend**: Only 'cluster' storage is currently supported via SQL
- **Collection modification**: Cannot add or remove tables from existing collections  
- **Concurrent backups**: Multiple backup operations on the same collection may conflict

For detailed performance considerations and chain management guidelines, see [Concepts](concepts.md).

## Next steps

- [Learn about backup collection concepts](concepts.md)
- [Explore all operations and management tasks](operations.md)

# Creating Your First Backup Collection

This guide walks you through creating a backup collection, taking your first backups, and monitoring backup operations.

## Creating a backup collection

Create a collection that includes the tables you want to back up together:

```sql
-- Create a collection for related tables
CREATE BACKUP COLLECTION production_backups
    ( TABLE orders
    , TABLE products
    , TABLE customers
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

## Taking backups

After creating the collection, take an initial full backup, then use incremental backups for subsequent operations:

```sql
-- Take initial full backup
BACKUP production_backups;

-- Later, take incremental backups
BACKUP production_backups INCREMENTAL;
```

## Monitoring backup operations

Track backup progress and browse your backup structure:

```bash
# Check backup operation status
ydb operation list incbackup

# Get details for specific operation
ydb operation get <operation-id>

# Browse backup collections
ydb scheme ls .backups/collections/

# List backups in a collection
ydb scheme ls .backups/collections/production_backups/
```

## Next steps

- [Setting Up Backups for Multiple Environments](multi-environment-setup.md)
- [Exporting Backups to External Storage](exporting-to-external-storage.md)
- [Validating and Testing Backups](validation-and-testing.md)

# Backup Collections: Common Recipes and Examples {#backup-collections-recipes}

This guide provides practical examples for common backup collection use cases. For basic operations, see the [operations guide](../maintenance/manual/backup-collections.md).

## Basic backup workflow {#basic-workflow}

### Creating your first backup collection {#creating-first-collection}

```sql
-- Create a collection for related tables
CREATE BACKUP COLLECTION `production_backups`
    ( TABLE `/Root/shop/orders`
    , TABLE `/Root/shop/products`
    , TABLE `/Root/shop/customers`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Taking backups {#taking-backups}

```sql
-- Take initial full backup
BACKUP `production_backups`;

-- Later, take incremental backups
BACKUP `production_backups` INCREMENTAL;
```

### Monitoring backup operations {#monitoring-backup-operations}

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

## Multi-environment setup {#multi-environment}

### Development environment {#development-environment}

```sql
-- Create collection with fewer tables for testing
CREATE BACKUP COLLECTION `dev_test_backups`
    ( TABLE `/Root/dev/users`
    , TABLE `/Root/dev/test_data`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Take daily full backups in development
BACKUP `dev_test_backups`;
```

### Production environment {#production-environment}

```sql
-- Create comprehensive collection for production
CREATE BACKUP COLLECTION `prod_daily_backups`
    ( TABLE `/Root/production/orders`
    , TABLE `/Root/production/products`
    , TABLE `/Root/production/customers`
    , TABLE `/Root/production/inventory`
    , TABLE `/Root/production/transactions`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Weekly full backup
BACKUP `prod_daily_backups`;

-- Daily incremental backups
BACKUP `prod_daily_backups` INCREMENTAL;
```

## Microservices backup strategy {#microservices}

### Service-specific collections {#service-specific-collections}

```sql
-- User service backup collection
CREATE BACKUP COLLECTION `user_service_backups`
    ( TABLE `/Root/users/profiles`
    , TABLE `/Root/users/preferences`
    , TABLE `/Root/users/sessions`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Order service backup collection
CREATE BACKUP COLLECTION `order_service_backups`
    ( TABLE `/Root/orders/orders`
    , TABLE `/Root/orders/order_items`
    , TABLE `/Root/orders/payments`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- Inventory service backup collection
CREATE BACKUP COLLECTION `inventory_service_backups`
    ( TABLE `/Root/inventory/products`
    , TABLE `/Root/inventory/stock_levels`
    , TABLE `/Root/inventory/warehouses`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

### Service backup workflow {#service-backup-workflow}

```sql
-- Take backups for each service independently
BACKUP `user_service_backups`;
BACKUP `order_service_backups`;
BACKUP `inventory_service_backups`;

-- Later, take incremental backups
BACKUP `user_service_backups` INCREMENTAL;
BACKUP `order_service_backups` INCREMENTAL;
BACKUP `inventory_service_backups` INCREMENTAL;
```

## Data export and recovery {#export-recovery}

### Exporting backup collections {#exporting-backup-collections}

For disaster recovery or migration, export backup collections using standard YDB tools:

```bash
# Export a backup collection
ydb tools dump -p .backups/collections/production_backups -o /backup/exports/production_backups_export

# Export specific backup from a collection
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /backup/exports/backup_20240315
```

### Importing to target database

```bash
# Import backup to target database (restores to backup collections)
ydb tools restore -i /backup/exports/production_backups_export -d .backups/collections/production_backups_restored

# Import specific backup into a collection
ydb tools restore -i /backup/exports/backup_20240315 -d .backups/collections/emergency_restore

# Note: Importing directly into .backups/collections/ must maintains backup collection structure
```

### Manual cleanup

```bash
```bash
# Remove old backup directories (manual cleanup)
ydb scheme rmdir -r .backups/collections/production_backups/20250821141425Z_full/

# Always remove complete chains, never partial chains
# Example: Remove old full backup and all its incrementals
ydb scheme rmdir -r .backups/collections/production_backups/20250821141425Z_full/
ydb scheme rmdir -r .backups/collections/production_backups/20250821141451Z_incremental/
ydb scheme rmdir -r .backups/collections/production_backups/20250822070000Z_incremental/
```

### Collection lifecycle

```sql
-- Drop a collection when no longer needed (removes collection and all backups)
DROP BACKUP COLLECTION `old_collection_name`;
```

## Simple backup validation {#validation}

### Verify backup completion

```bash
# Check if backup operation completed successfully
ydb operation list incbackup | grep -E "(COMPLETED|FAILED)"

# Verify backup directory exists
ydb scheme ls .backups/collections/production_backups/ | tail -1
```

### Basic backup testing

```bash
# Export a recent backup for testing
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /tmp/test_restore

# Test restore to temporary location (in test environment)
ydb tools restore -i /tmp/test_restore -d /Root/test_restore_verification
```

## See also {#see-also}

- [Backup collections concepts](../concepts/backup-collections.md) - Core concepts and architecture.
- [Operations guide](../maintenance/manual/backup-collections.md) - Detailed operational procedures.

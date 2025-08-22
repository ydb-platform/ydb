# Backup Collections Operations {#backup-collections-operations}

This guide covers all practical operations for creating, managing, and restoring from backup collections. For conceptual information, see [Backup collections concepts](../../concepts/backup-collections.md).

## Creating backup collections {#creating-collections}

### SQL Syntax Reference {#sql-syntax-reference}

```sql
-- CREATE BACKUP COLLECTION syntax
CREATE BACKUP COLLECTION `collection_name`
    ( TABLE `table_path` [, TABLE `table_path` ...] )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- BACKUP syntax
BACKUP `collection_name` [INCREMENTAL];

-- DROP BACKUP COLLECTION syntax
DROP BACKUP COLLECTION `collection_name`;

-- RESTORE syntax

RESTORE `collection_name`
```

For detailed syntax, see [YQL reference documentation](../../yql/reference/syntax/index.md).

### Basic collection creation {#basic-collection-creation}

Create a backup collection using SQL commands. The collection defines which tables to include and storage settings:

```sql
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/shop/orders`, TABLE `/Root/shop/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );
```

## Taking backups {#taking-backups}

### Initial full backup {#initial-full-backup}

After creating a collection, take the initial full backup:

```sql
BACKUP `shop_backups`;
```

The first backup without the `INCREMENTAL` keyword creates a full backup containing all data from the specified tables.

### Incremental backups {#incremental-backups}

Once you have a full backup, create incremental backups to capture changes:

```sql
BACKUP `shop_backups` INCREMENTAL;
```

### Backup timing considerations {#backup-timing-considerations}

Implement regular backup schedules based on your requirements. Note that scheduling must be implemented externally using cron or similar tools.

**Daily full backups:**

```sql
-- Example: Run daily at 2 AM (requires external scheduling)
BACKUP `shop_backups`;
```

**Hourly incrementals with weekly full backups:**

```sql
-- Example: Sunday: Full backup (requires external scheduling)
BACKUP `shop_backups`;

-- Example: Monday-Saturday: Incremental backups (requires external scheduling)
BACKUP `shop_backups` INCREMENTAL;
```

### Important scheduling notes {#important-scheduling-notes}

- **External scheduling required**: YDB does not provide built-in scheduling. Use cron or similar tools.
- **Background operations**: Backups run asynchronously and don't block database operations.
- **Multiple collections**: Can run backups on different collections independently.

## Monitoring backup operations {#monitoring}

### Monitoring backup operations {#monitoring-backup-operations}

```bash
# Check backup operation status
ydb operation list incbackup

# Get details for specific operation
ydb operation get <operation-id>

# Cancel running operation if needed
ydb operation cancel <operation-id>

# Browse backup collections
ydb scheme ls .backups/collections/

# List backups in a collection
ydb scheme ls .backups/collections/production_backups/

# Get backup metadata
ydb scheme describe .backups/collections/production_backups/backup_20240315_120000/
```

### Check operation status {#check-operation-status}

All backup operations run in the background. Monitor their progress using:

```bash
# List all backup operations
ydb operation list incbackup

# Check specific operation status
ydb operation get <operation-id>
```

### Browse backup structure {#browse-backup-structure}

View backups through the database schema:

```bash
# List all collections
ydb scheme ls .backups/collections/

# View specific collection structure
ydb scheme ls .backups/collections/shop_backups/

# Check backup timestamps
ydb scheme ls .backups/collections/shop_backups/ | sort
```

### Monitor backup chains {#monitor-backup-chains}

Verify backup chain integrity:

```bash
# List backups in chronological order
ydb scheme ls .backups/collections/shop_backups/ | sort

# Check individual backup contents
ydb scheme ls .backups/collections/shop_backups/20250821141519Z_full
```

## Managing backup collections {#managing-collections}

### Collection information {#collection-information}

Since collections are managed through SQL, browse them using schema commands:

```bash
# Browse all collections
ydb scheme ls .backups/collections/

# View collection directory contents
ydb scheme describe .backups/collections/shop_backups/
```

### Collection status monitoring {#collection-status-monitoring}

Check the health and status of your collections:

1. **Verify recent backups exist**.
2. **Check backup chain completeness**.
3. **Monitor storage usage**.
4. **Validate backup accessibility**.

## Retention and cleanup {#retention-cleanup}

### Backup chain considerations {#backup-chain-considerations}

Before deleting backups, understand chain dependencies:

- **Full backups**: Required for all subsequent incrementals.
- **Incremental backups**: Depend on the latest full backup and all subsequent incrementals in the chain.
- **Chain breaks**: Deleting intermediate backups breaks the chain.

### Manual cleanup strategies {#manual-cleanup-strategies}

**Safe cleanup approach:**

1. Create new full backup.
2. Verify new backup is complete.
3. Delete old backup chains (full backup + all its incrementals).
4. Never delete partial chains.

**Example cleanup workflow:**

```bash
# 1. Create new full backup
ydb yql -s "BACKUP \`shop_backups\`;"

# 2. Wait for completion and verify
ydb operation list incbackup

# 3. Browse backup structure
ydb scheme ls .backups/collections/shop_backups/ | sort

# 4. Remove old backup directories (entire chains only)
ydb scheme rmdir -r .backups/collections/shop_backups/20250208141425Z_full/
ydb scheme rmdir -r .backups/collections/shop_backups/20250209141519Z_incremental/
ydb scheme rmdir -r .backups/collections/shop_backups/20250210141612Z_incremental/
# ... (remove all related incrementals in the chain)
```

### Retention policies {#retention-policies}

Implement retention policies based on:

- **Business requirements**: How long data must be retained.
- **Storage costs**: Balance retention with storage usage.
- **Recovery needs**: Typical recovery scenarios and timeframes.
- **Compliance**: Legal or regulatory requirements.

**Example retention policy:**

- Keep daily backups for 30 days.
- Keep weekly backups for 12 weeks.
- Keep monthly backups for 12 months.
- Keep yearly backups for 7 years.

## Backup validation and verification {#validation}

### Post-backup validation {#post-backup-validation}

After backup completion:

1. **Verify operation success**: Check operation status.
2. **Validate backup structure**: Browse backup directories.

### Backup testing procedures {#backup-testing-procedures}

Regularly test backup restoration:

1. **Test environment**: Use separate test environment.
2. **Sample restoration**: Restore subset of data.
3. **Full restoration test**: Periodically test complete restoration.
4. **Performance testing**: Measure restoration time and resources.

## Recovery and restoration {#recovery}

### Export and import restoration {#export-import-restoration}

For complete disaster recovery, use export/import operations:

```bash
# Export backup collection from source
ydb tools dump -p .backups/collections/shop_backups -o shop_backups_export

# create backup collection in target database
# CREATE BACKUP COLLECTION `collection_name`
#     ( TABLE `table_path` [, TABLE `table_path` ...] )
# WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

# Import to target database
ydb tools restore -i shop_backups_export -d /Root/restored_db
```

### Point-in-time recovery {#point-in-time-recovery}

To restore to a specific point in time:

1. **Identify target backup**: Find the appropriate backup point.
2. **Export backup chain**: Import the full backup and required incrementals.
3. **Execute RESTORE**.

## Best practices summary {#best-practices}

### Backup strategy {#backup-strategy}

- **Regular schedule**: Establish consistent backup timing.
- **Manage chain length**: Take new full backups periodically to avoid excessively long incremental chains.
- **Multiple collections**: Separate collections for different applications.

### Monitoring and maintenance {#monitoring-maintenance}

- **Manual validation**: Periodically test backup restoration.
- **Performance tracking**: Monitor backup duration and resource usage.
- **Storage monitoring**: Track backup storage growth.

## See also {#see-also}

- [Backup collections concepts](../../concepts/backup-collections.md) - Core concepts and architecture.
- [Common recipes](../../recipes/backup-collections.md) - Real-world usage examples.

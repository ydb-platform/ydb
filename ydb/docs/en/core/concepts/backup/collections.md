# Backup collections and incremental backups

Backup collections provide an advanced backup solution for {{ ydb-short-name }} that organizes full and incremental backups into managed collections. This approach is designed for production workloads requiring efficient disaster recovery and point-in-time recovery capabilities.

## Overview

Backup collections solve several challenges with traditional export/import approaches:

- **Efficiency**: Incremental backups capture only changes since the previous backup.
- **Organization**: Related backups are grouped into logical collections.
- **Recovery flexibility**: Enables point-in-time recovery to any backup in the chain.

## Core concepts

### Backup collection

A named set of backups for selected database tables. Collections organize related backups and ensure they can be restored together consistently.

### Full backup

A complete snapshot of all selected tables at a specific point in time. Serves as the baseline for subsequent incremental backups.

### Incremental backup

Captures only the changes (inserts, updates, deletes) since the previous backup in the chain. Significantly smaller than full backups for datasets with limited changes.

### Backup chain

An ordered sequence of backups starting with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain.

## Architecture

### Backup flow

1. **Create collection**: Define which tables to include
2. **Initial full backup**: Create baseline snapshot
3. **Regular incremental backups**: Capture ongoing changes
4. **Retention management**: Remove old backups while preserving chain integrity

### Storage options

- **Cluster storage**: Backups stored within the {{ ydb-short-name }} cluster (current default).
- **External storage**: Manual export to filesystem or S3 using CLI tools for long-term archival (automatic external storage support may be added in future versions).

### Background operations

All backup and restore operations run asynchronously in the background, allowing you to monitor progress through the long operations API without blocking other database activities.

## When to use backup collections

**Ideal scenarios:**

- Production environments requiring regular backups
- Large datasets where incremental changes are much smaller than full data size
- Point-in-time recovery requirements
- Disaster recovery planning with RTO/RPO constraints

**Consider traditional export/import and backup/restore for:**

- Small databases or single tables
- Databases with high change velocity relative to stored data volume
- One-time data migration tasks
- Development/testing environments
- Simple backup scenarios without incremental needs

## Quick start

To get started with backup collections:

```sql
-- 1. Create a collection for your tables
CREATE BACKUP COLLECTION `my_backup_collection`
    ( TABLE `/Root/database/orders`
    , TABLE `/Root/database/products`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Create initial full backup
BACKUP `my_backup_collection`;

-- 3. Create incremental backups as needed
BACKUP `my_backup_collection` INCREMENTAL;
```

For detailed command reference and advanced usage, see [Backup collections CLI guide](../../reference/ydb-cli/export-import/backup-collections/index.md).

## See also

- [Backup concepts overview](../backup.md) - General backup approaches in {{ ydb-short-name }}
- [Backup collections CLI reference](../../reference/ydb-cli/export-import/backup-collections/index.md) - Detailed command reference and examples
- [Export and import reference](../../reference/ydb-cli/export-import/index.md) - Traditional backup methods
- [Backup and recovery guide](../../devops/backup-and-recovery.md) - Comprehensive backup strategies

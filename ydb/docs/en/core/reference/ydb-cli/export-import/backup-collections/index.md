# Backup collections

Efficient incremental backup and point-in-time recovery for YDB tables.

## Quick start

```sql
-- 1. Create collection
CREATE BACKUP COLLECTION `my_backups`
    ( TABLE `/Root/db/table1`, TABLE `/Root/db/table2` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

-- 2. Take full backup
BACKUP `my_backups`;

-- 3. Take incremental backups
BACKUP `my_backups` INCREMENTAL;
```

## Key features

- **Incremental backups** - Store only changes, reducing storage requirements.
- **Point-in-time recovery** - Restore to any backup point in the chain.
- **SQL API** - Manage backups with familiar SQL commands.

## Documentation

- [Concepts](concepts.md) — Architecture, backup types, and storage backends.
- [Operations](operations.md) — Complete guide to creating, managing, and restoring.
- [SQL API](sql-api.md) — SQL command reference.
- [YQL Syntax Reference](../../../../yql/reference/syntax/backup-collections.md) — Complete SQL syntax documentation.
- [Operation Monitoring](../../operation-list.md) — Monitor backup operations progress.

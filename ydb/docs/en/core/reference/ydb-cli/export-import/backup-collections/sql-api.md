# SQL API: Backup collections

Syntax

```sql
CREATE BACKUP COLLECTION <name>
  ( TABLE <path> [, ...] )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true'|'false' );

BACKUP <name> [INCREMENTAL];
```

Semantics

- First BACKUP without INCREMENTAL creates a full; subsequent with INCREMENTAL capture changes.
- Collections define the table set; only listed tables are backed up.

Examples

```sql
CREATE BACKUP COLLECTION `shop_backups`
  ( TABLE `/Root/test1/orders`, TABLE `/Root/test1/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

BACKUP `shop_backups`;
BACKUP `shop_backups` INCREMENTAL;
```

Notes

- Current backend: 'cluster'. Export/import to FS or S3 is done with CLI tools.
- Restore will create missing tables/collections when importing backups.

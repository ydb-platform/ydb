# Create a backup collection and initial full

You can create a collection with the SQL API and take the initial full backup.

SQL (current)
```sql
CREATE BACKUP COLLECTION `shop_backups`
    ( TABLE `/Root/test1/orders`
    , TABLE `/Root/test1/products`
    )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

BACKUP `shop_backups`;
```

Notes
- First run of BACKUP without INCREMENTAL creates a full backup.
- Collection table list defines the scope of backup.
- For exporting backups to FS/S3, see the export/import section.

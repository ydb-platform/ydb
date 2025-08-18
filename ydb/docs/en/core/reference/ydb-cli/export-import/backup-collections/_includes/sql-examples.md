SQL examples

Create collection and take backups
```sql
CREATE BACKUP COLLECTION `shop_backups`
  ( TABLE `/Root/test1/orders`, TABLE `/Root/test1/products` )
WITH ( STORAGE = 'cluster', INCREMENTAL_BACKUP_ENABLED = 'true' );

BACKUP `shop_backups`;
BACKUP `shop_backups` INCREMENTAL;
```

# Incremental backups

Incremental backups record the changes since the previous backup in the collection.

SQL (current)

```sql
BACKUP `shop_backups` INCREMENTAL;
```

Guidelines

- Keep chains reasonably short; schedule periodic full or synthetic full (if supported) to cap restore time.
- Make sure data changes between backups are committed before triggering BACKUP.
- Monitor backup status and integrity before relying on a chain.

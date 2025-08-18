# Restore from a backup collection

This page explains how to restore data using backups from a collection.

Export backups (source database)
```bash
# Export all backups from a collection (example)
ydb tools dump -p .backups/collections/shop_backups -o shop_backups_export
```

Import into destination (no shell scripting required)
- Create or select a new backup collection in the target DB for the restore.
- Import only the backups you want to apply, in chronological order:
  - Full only (Checkpoint 1): import the full backup.
  - Full + first incremental (Checkpoint 2): import the full backup, then the first incremental.
  - Full + all incrementals (Checkpoint 3): import the full backup, then all incrementals.
- The restore tool will create missing tables and the target collection if needed.
- Tip: backup directories include timestamps; lexicographic order equals chronological order.

Validation
- Run counts/checksums queries.
- Spot-check key rows and schema.

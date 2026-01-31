# Importing and Restoring Backups

Import backups from external storage and restore data to your database.

## Importing from filesystem

Import previously exported backups:

```bash
# Import backup to target database
ydb tools restore -i /backup/exports/production_backups_export -d .backups/collections/production_backups_restored

# Import specific backup into a collection
ydb tools restore -i /backup/exports/backup_20240315 -d .backups/collections/emergency_restore
```

## Importing from S3

```bash
ydb import s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket my-backup-bucket \
  --item backups/production_backups,.backups/collections/production_backups
```

## Restoring data

After importing backups to the cluster, restore the data:

```sql
RESTORE production_backups;
```

## Disaster recovery workflow

1. **Import full backup**: Import the base full backup first
2. **Import incrementals**: Import each incremental backup in order
3. **Execute RESTORE**: Run the RESTORE command to apply the backup chain

```bash
# Step 1: Import full backup
ydb tools restore -i /backup/full_20250821 -d .backups/collections/recovery/20250821141425Z_full

# Step 2: Import incrementals in order
ydb tools restore -i /backup/inc_20250822 -d .backups/collections/recovery/20250822070000Z_incremental

# Step 3: Restore
ydb yql -s "RESTORE recovery;"
```

## Next steps

- [Backup Maintenance and Cleanup](maintenance-and-cleanup.md)
- [Validating and Testing Backups](validation-and-testing.md)

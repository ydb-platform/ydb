# Exporting Backups to External Storage

Export backup collections to S3-compatible storage or filesystem for disaster recovery.

## Export to S3

For large backups or disaster recovery, export directly to S3-compatible storage:

```bash
# Export a backup collection to S3
ydb export s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket my-backup-bucket \
  --item .backups/collections/production_backups,backups/production_backups

# Export specific backup to S3
ydb export s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket my-backup-bucket \
  --item .backups/collections/production_backups/20250821141425Z_full,backups/20250821141425Z_full
```

## Export to filesystem

For smaller backups or local testing, export to local filesystem:

```bash
# Export a backup collection
ydb tools dump -p .backups/collections/production_backups -o /backup/exports/production_backups_export

# Export specific backup from a collection
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /backup/exports/backup_20240315
```

## Export best practices

- **Export each backup separately**: Export full backups and incrementals individually to maintain chain integrity
- **Preserve chain order**: When exporting a chain, export the full backup first, then incrementals in order
- **Verify exports**: Check that exported data is complete before deleting cluster-stored backups
- **Schedule regular exports**: Automate exports to external storage for disaster recovery

## Next steps

- [Importing and Restoring Backups](importing-and-restoring.md)
- [Backup Maintenance and Cleanup](maintenance-and-cleanup.md)

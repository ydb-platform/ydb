# Validating and Testing Backups

Verify backup integrity and test restoration procedures.

## Verify backup completion

Check that backup operations completed successfully:

```bash
# Check if backup operation completed successfully
ydb operation list incbackup | grep -E "(COMPLETED|FAILED)"

# Verify backup directory exists
ydb scheme ls .backups/collections/production_backups/ | tail -1
```

## Verify external exports

Regularly verify that backups can be exported to external storage:

```bash
# Export to S3 and verify
ydb export s3 \
  --s3-endpoint storage.yandexcloud.net \
  --bucket my-backup-bucket \
  --item .backups/collections/production_backups/20250821141425Z_full,backups/test_export

# Check export completed successfully
ydb operation list export/s3

# Verify exported data exists in S3 (using your S3 client)
# aws s3 ls s3://my-backup-bucket/backups/test_export/
```

## Test backup restoration

Regularly test that backups can be restored:

```bash
# Export a recent backup for testing
ydb tools dump -p .backups/collections/production_backups/backup_20240315_120000 -o /tmp/test_restore

# Test restore to temporary location (in test environment)
ydb tools restore -i /tmp/test_restore -d /Root/test_restore_verification
```

## Validation checklist

- [ ] Backup operations complete without errors
- [ ] Backup directories are created with expected timestamps
- [ ] Exported backups can be imported successfully
- [ ] Restored data matches expected content
- [ ] Incremental chain integrity is maintained

## Recommended testing schedule

| Test Type | Frequency | Description |
|-----------|-----------|-------------|
| Operation status | Daily | Check that scheduled backups complete |
| Export verification | Weekly | Verify exports to external storage |
| Full restore test | Monthly | Complete restore to test environment |
| Disaster recovery drill | Quarterly | Full DR simulation |

## Next steps

- [Creating Your First Backup Collection](getting-started.md)
- [Backup Maintenance and Cleanup](maintenance-and-cleanup.md)

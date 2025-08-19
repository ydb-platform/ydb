# Backup collections and incremental backups

Availability: YDB vX.Y+ (TBD)

Summary

- Organize full and incremental backups into collections for efficient DR and PITR.
- Works with cluster-managed storage today; export/import to FS or S3 via CLI tools.
- Background operations with status monitoring through long operations API.

Glossary

- Backup collection: Named set of backups for selected tables.
- Full backup: Baseline snapshot for a collection.
- Incremental backup: Changes since the previous backup in the chain.
- Synthetic full: Consolidated full produced from existing chain (TBD support).
- Chain: Ordered sequence full → incrementals with dependencies.
- Backup storage vs backup target: Where backups live vs what is being backed up.
- Retention policy: Rules to remove old backups without breaking chains.

Architecture overview

- Flow: create collection → full → incrementals → optional synthetic full → retention cleanup.
- Storage layout: cluster (current), export/import to FS or S3. Metadata files include metadata.json and mapping.json.
- Compatibility: See reference/compatibility for format versions and cross-version notes.

When to use

- Disaster Recovery and Point-in-Time Recovery.
- Large datasets where incremental changes are much smaller than full snapshots.

## Creating and managing collections

### Creating a backup collection

```sql
-- Create collection with specified tables
CREATE BACKUP COLLECTION `my_backup_collection`
    ( TABLE `/Root/database/orders`
    , TABLE `/Root/database/products`
    )
WITH
    ( STORAGE = 'cluster'
    );
```

## Performing backups

### Creating a full backup

```sql
-- Create full backup of collection
BACKUP `my_backup_collection`;
```

### Creating an incremental backup

```sql
-- Create incremental backup
BACKUP `my_backup_collection` INCREMENTAL;
```

## Export and import collections

### Export collection to filesystem

```bash
# Export collection using tools dump
ydb tools dump -p .backups/collections/my_backup_collection -o exported_backups

# Analyze created backups
ls -la exported_backups/
```

### Import collection from filesystem

```bash
# Restore full backup
for backup_dir in exported_backups/*/; do
    backup_name=$(basename "$backup_dir")
    case "$backup_name" in
        *_full*)
            echo "Importing full backup: $backup_name"
            ydb tools restore -p ".backups/collections/target_collection/$backup_name" -i "$backup_dir"
            ;;
        *_incremental*)
            echo "Importing incremental backup: $backup_name"
            ydb tools restore -p ".backups/collections/target_collection/$backup_name" -i "$backup_dir"
            ;;
    esac
done
```

## Operations monitoring

Incremental backup and restore operations run in the background. To track their status, long operations are used:

### Incremental backup operations (`incbackup`)

When creating an incremental backup, a background operation of type `incbackup` is started. Get list of all incremental backup operations:

```bash
{{ ydb-cli }} operation list incbackup
```

Get status of specific operation:

```bash
{{ ydb-cli }} operation get "ydb://incbackup/N?id=<operation-id>"
```

### Restore operations (`restore`)

When restoring from backup collections, a background operation of type `restore` is started. Get list of all restore operations:

```bash
{{ ydb-cli }} operation list restore
```

Get status of specific operation:

```bash
{{ ydb-cli }} operation get "ydb://restore/N?id=<operation-id>"
```

### Managing operations

After successful completion of an operation, it is recommended to remove it from the list:

```bash
{{ ydb-cli }} operation forget "ydb://incbackup/N?id=<operation-id>"
{{ ydb-cli }} operation forget "ydb://restore/N?id=<operation-id>"
```

Operation IDs are returned when starting corresponding backup or restore commands and can be used to track progress and execution status.

## Limitations and recommendations

### Current limitations

- Maximum incremental backup chain length: 100
- Maximum single operation size: 1 TB
- Parallel operations: no more than 3 per collection
- PITR granularity: 1 second

### Performance recommendations

- Schedule full backups during low-load periods
- Use separate collections for different table sets
- Monitor chain sizes and restore times

### Version compatibility

- Format v1: YDB 24.3+
- Format v2: YDB 25.1+ (with compression support)
- Backward compatibility: can restore v1 in v2, but not vice versa

## See also

- [Backup basics](../backup.md)
- [Export and import command reference](../../reference/ydb-cli/export-import/index.md)
- [Backup collections - detailed guide](../../reference/ydb-cli/export-import/backup-collections/index.md)

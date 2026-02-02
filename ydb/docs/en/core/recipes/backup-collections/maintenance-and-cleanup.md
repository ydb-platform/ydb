# Backup Maintenance and Cleanup

Manage backup lifecycle and clean up old backups to control storage usage.

## Identifying backup chains

Backup directories are named with timestamps and suffixes (`_full` or `_incremental`). Incremental backups belong to the most recent full backup that precedes them chronologically.

```bash
# List backups sorted by time
ydb scheme ls .backups/collections/production_backups/ | sort
```

Example output:
```
20250208141425Z_full        # Chain 1: full backup
20250209141519Z_incremental # Chain 1: incremental (belongs to 20250208 full)
20250210141612Z_incremental # Chain 1: incremental (belongs to 20250208 full)
20250215120000Z_full        # Chain 2: new full backup starts new chain
20250216140000Z_incremental # Chain 2: incremental (belongs to 20250215 full)
```

When you create a new full backup, it starts a new chain. All incrementals created after that full backup (until the next full) belong to that chain.

## Manual cleanup

Remove old backup chains when they are no longer needed:

```bash
# Remove old backup directories
ydb scheme rmdir -r .backups/collections/production_backups/20250821141425Z_full/

# Always remove complete chains, never partial chains
# Example: Remove old full backup and all its incrementals
ydb scheme rmdir -r .backups/collections/production_backups/20250821141425Z_full/
ydb scheme rmdir -r .backups/collections/production_backups/20250821141451Z_incremental/
ydb scheme rmdir -r .backups/collections/production_backups/20250822070000Z_incremental/
```

{% note warning %}

Never delete individual backups from the middle of a chain. Deleting a full backup makes all its incremental backups unrestorable. Always delete complete chains together.

{% endnote %}

## Dropping a collection

Remove an entire backup collection when it's no longer needed:

```sql
-- Drop a collection when no longer needed (removes collection and all backups)
DROP BACKUP COLLECTION old_collection_name;
```

## Safe cleanup workflow

1. Create a new full backup
2. Verify the new backup is complete
3. Export old backup chains to external storage if needed
4. Delete old backup chains (full backup + all its incrementals together)

## Storage management tips

- **Monitor storage growth**: Track backup storage usage regularly
- **Set retention policies**: Define how long to keep backup chains
- **Start new chains periodically**: Create new full backups weekly or bi-weekly to limit chain length
- **Export before deleting**: Always export to external storage before removing cluster backups

## Next steps

- [Validating and Testing Backups](validation-and-testing.md)
- [Exporting Backups to External Storage](exporting-to-external-storage.md)

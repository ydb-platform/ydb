# Backup Maintenance and Cleanup

Manage backup lifecycle and clean up old backups to control storage usage.

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

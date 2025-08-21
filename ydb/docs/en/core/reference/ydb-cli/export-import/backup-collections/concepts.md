# Backup collections concepts

This section explains operational details about backup collections, storage backends, and chain management. For architectural overview and core concepts, see [Backup collections](../../../../concepts/backup/collections.md).

## Backup chain validation and integrity {#backup-chains-integrity}

When working with backup collections, understanding chain dependencies is critical for successful operations:

### Chain validity rules {#chain-validity-rules}

- **Sequential dependency**: Each incremental backup depends on all previous backups in the chain.
- **Deletion constraints**: Removing any backup in the middle of a chain breaks the chain for subsequent backups.
- **Restoration requirements**: To restore from an incremental backup, you need the full backup plus all preceding incremental backups.

### Chain management best practices

- **Monitor chain length**: Keep backup chains reasonably short (7-14 incremental backups recommended).
- **Plan retention carefully**: Always consider chain dependencies when cleaning up old backups.
- **Verify before deletion**: Use schema browsing to understand backup structure before removing backups.

## Storage backends {#storage-backends}

### Cluster storage

Stores backups within the YDB cluster itself (current implementation).

```sql
WITH ( STORAGE = 'cluster' )
```

For external storage (filesystem, S3), use [export/import operations](operations.md#restore-operations).

## Backup chains and integrity {#backup-chains-integrity}

### Chain structure

A backup chain consists of:

1. **One full backup** (the chain foundation)
2. **Zero or more incremental backups** (applied in chronological order)

### Chain validity rules {#chain-validity-rules}

{% note alert %}

Never delete full backups that have dependent incremental backups. Deleting a full backup breaks the entire chain, making all subsequent incrementals unrestorable.

{% endnote %}

**Best practices:**

- Apply retention policies carefully to preserve chain validity.
- Verify backup chains periodically before critical operations.
- Respect chronological order when applying backups during restore.

### Maximum chain length recommendations (TBD)

- **Daily incrementals**: Limit chains to 7-14 incremental backups
- **Hourly incrementals**: Limit chains to 24-48 incremental backups
- **High-frequency backups**: Consider synthetic full backups to reset chains

## Limitations and requirements {#limitations-requirements}

### Current limitations

- Cluster storage is the primary backend; filesystem and S3 require export/import operations.
- Chain order must be respected - cannot skip or reorder backups during restore.

## Next steps

- [Learn how to create and manage collections](operations.md).
- [Explore the complete SQL API](sql-api.md).

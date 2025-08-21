# Backup collections concepts

This section explains how backup collections work, the types of backups available, and the storage backends you can use.

## How backup collections work {#how-backup-collections-work}

A backup collection is a named set of backups for selected tables that maintains a chronological chain of data snapshots. Each collection contains:

- **A chain of backups**: One full backup followed by zero or more incremental backups
- **Storage settings**: Configuration options that define backup storage backend
- **Table mapping**: Definitions that specify which tables belong to the collection

The backup chain allows you to restore your data to any point in time by applying the full backup and subsequent incremental backups up to your desired restore point.

## Types of backups {#types-of-backups}

### Full backups

A full backup contains a complete snapshot of all data in the collection at a specific point in time. Key characteristics:

- **Complete data capture**: Contains all rows from all tables in the collection
- **Self-contained**: Can be restored independently without other backups
- **Foundation for chains**: Serves as the base for subsequent incremental backups
- **Storage intensive**: Requires more storage space but faster to restore

**When to use full backups:**

- Initial backup creation
- After significant data changes
- To reset backup chains and reduce restore time
- For critical checkpoint creation

### Incremental backups

Incremental backups capture only the changes made since the previous backup in the chain. Key characteristics:

- **Change-based**: Records insertions, updates, and deletions since the last backup
- **Storage efficient**: Requires minimal storage space for typical workloads
- **Chain dependent**: Requires the full backup and all preceding incremental backups for restore
- **Fast execution**: Completes quickly for typical change volumes

**When to use incremental backups:**

- For regular scheduled backups (daily, hourly)
- When storage efficiency is important
- To capture frequent changes with minimal overhead

See [Operations guide](operations.md#taking-backups) for detailed guidelines.

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

- Apply retention policies carefully to preserve chain validity
- Verify backup chains periodically before critical operations
- Respect chronological order when applying backups during restore

### Maximum chain length recommendations (TBD)

- **Daily incrementals**: Limit chains to 7-14 incremental backups
- **Hourly incrementals**: Limit chains to 24-48 incremental backups
- **High-frequency backups**: Consider synthetic full backups to reset chains

## Limitations and requirements {#limitations-requirements}

### Current limitations

- Cluster storage is the primary backend; filesystem and S3 require export/import operations
- Chain order must be respected - cannot skip or reorder backups during restore

## Next steps

- [Learn how to create and manage collections](operations.md)
- [Explore the complete SQL API](sql-api.md)

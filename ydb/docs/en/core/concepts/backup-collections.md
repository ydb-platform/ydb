# Backup Collections {#backup-collections}

Backup collections provide an advanced backup solution for YDB that organizes full and incremental backups into managed collections. This approach is designed for production workloads requiring efficient disaster recovery and point-in-time recovery capabilities.

## What are backup collections? {#what-are-backup-collections}

A backup collection is a named set of coordinated backups for selected database tables. Collections organize related backups and ensure they can be restored together consistently, providing:

- **Efficiency**: Incremental backups capture only changes since the previous backup.
- **Organization**: Related backups are grouped into logical collections.
- **Recovery flexibility**: Enables recovery using any backup in the chain.

## Core concepts {#core-concepts}

### Backup collection {#backup-collection}

A named container that groups backups for a specific set of database tables. Collections ensure that all included tables are backed up consistently.

### Full backup {#full-backup}

A complete snapshot of all selected tables at a specific point in time. Serves as the baseline for subsequent incremental backups and contains all data needed for independent restoration.

### Incremental backup {#incremental-backup}

Captures only the changes (inserts, updates, deletes) since the previous backup in the chain. Significantly smaller than full backups for datasets with limited changes.

### Backup chain {#backup-chain}

An ordered sequence of backups starting with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain for complete restoration.

## Architecture and components {#architecture}

### Backup flow {#backup-flow}

1. **Collection creation**: Define which tables to include and storage settings
2. **Initial full backup**: Create baseline snapshot of all tables
3. **Regular incremental backups**: Capture ongoing changes on-demand
4. **Chain management**: Monitor backup chains and manage retention manually

### Storage structure {#storage-structure}

Backup collections are stored in a dedicated directory structure within the database:

```text
/Root/test1/.backups/collections/
├── backup_collection_1/
│   ├── 20250821141425Z_full/       # Full backup
│   │   ├── table_1/
│   │   └── table_2/
│   └── 20250821141519Z_incremental/ # Incremental backup
│       ├── table_1/
│       └── table_2/
└── backup_collection_2/
    ├── 20250820093012Z_full/       # Full backup
    │   └── table_3/
    ├── 20250820140000Z_incremental/ # First incremental
    │   └── table_3/
    └── 20250821080000Z_incremental/ # Second incremental
        └── table_3/
```

Each backup contains:

- Table schemas at backup time. (Implicitly)
- Data files (full or incremental changes).

### Storage backends {#storage-backends}

#### Cluster storage {#cluster-storage}

Backups are stored within the YDB cluster itself, providing:

- **High availability**: Leverages cluster replication and fault tolerance.
- **Performance**: Fast backup and restore operations.
- **Integration**: Seamless integration with cluster operations.
- **Security**: Uses cluster security mechanisms.

```sql
WITH ( STORAGE = 'cluster' )
```

#### External storage {#external-storage}

Currently, external storage requires manual export/import operations. Use [export/import operations](../reference/ydb-cli/export-import/index.md) to move backups to external storage systems.

### Background operations {#background-operations}

All backup operations run asynchronously in the background, allowing you to:

- Continue normal database operations during backups.
- Monitor progress using YDB CLI operation commands.
- Handle large datasets without blocking other activities.

## How backup collections work internally {#how-they-work}

### Backup creation process {#backup-creation-process}

1. **Transaction isolation**: Backup starts from a consistent snapshot point
2. **Change tracking**: For incremental backups, only changes since last backup are captured and stored in CDC stream
3. **Change materialization**: When incremental backup called CDC stream compacted to incremental backup tables

### Incremental backup mechanism {#incremental-backup-mechanism}

Incremental backups use change tracking to identify:

- **New rows**: Added since last backup.
- **Modified rows**: Changed data in existing rows.  
- **Deleted rows**: Removed data (tombstone records).
- **Schema changes**: Currently not supported.

## Relationship with incremental backups {#relationship-with-incremental-backups}

Backup collections are the foundation for incremental backup functionality:

- **Collections enable incrementals**: You must have a collection to create incremental backups.
- **Chain management**: Collections manage the sequence of full and incremental backups.
- **Consistency**: All tables in a collection are backed up consistently.

Without backup collections, only full export/import operations are available.

## When to use backup collections {#when-to-use}

**Ideal scenarios:**

- Production environments requiring regular backup schedules.
- Large datasets where incremental changes are much smaller than total data size.
- Scenarios requiring backup chains for efficiency.

**Consider traditional export/import for:**

- Small databases or individual tables.
- One-time data migration tasks.
- Development/testing environments.
- Simple backup scenarios without incremental needs.

## Benefits and limitations {#benefits-limitations}

### Benefits

- **Storage efficiency**: Incremental backups use significantly less storage.
- **Faster backups**: Only changes are processed after initial full backup (note: change capture still incurs storage and cpu costs).
- **SQL interface**: Familiar SQL commands for backup management.
- **Background processing**: Non-blocking operations.

### Current limitations

- **Cluster storage only**: External storage requires manual export/import.
- **No collection modification**: Cannot add/remove tables after creation.
- **No partial restore**: Partial restores from collections must be managed externally.

## Next steps {#next-steps}

- **Get started**: Follow the [operations guide](../maintenance/manual/backup-collections.md) for step-by-step instructions
- **See examples**: Explore [common scenarios](../recipes/backup-collections.md) and best practices

## See also

- [General backup concepts](backup.md) - Overview of all backup approaches in YDB.
- [Operations guide](../maintenance/manual/backup-collections.md) - Practical instructions and examples.
- [Common recipes](../recipes/backup-collections.md) - Real-world usage scenarios.

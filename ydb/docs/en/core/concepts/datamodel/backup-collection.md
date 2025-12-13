# Backup Collection

A backup collection is a [schema object](index.md) that enables [point-in-time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery) for selected [row-oriented tables](table.md#row-oriented-tables). Backup collections organize full and incremental backups into managed chains, allowing efficient disaster recovery and protection against accidental data loss such as erroneous deletions or modifications.

{% note info %}

For practical instructions on creating and managing backup collections, see the [Backup and Recovery guide](../../devops/backup-and-recovery/index.md#backup-collections).

{% endnote %}

## Overview

Backup collections address common backup challenges for production workloads:

- **Storage efficiency**: Incremental backups capture only changes since the previous backup, significantly reducing storage requirements compared to multiple full backups.
- **Consistent recovery**: All tables in a collection are backed up from the same global snapshot, ensuring referential integrity across tables during restoration.
- **Point-in-time recovery**: Restore data to any point when a backup was created within the chain.

For a comparison with other backup methods (export/import, dump/restore), see the [Backup and Recovery guide](../../devops/backup-and-recovery/index.md).

## Key Concepts

These terms are essential to understanding backup collections. For detailed definitions, see the [glossary](../glossary.md#backup-collection).

- **[Full backup](../glossary.md#backup)**: A complete snapshot of all data in the collection at a specific point in time. Serves as the foundation for subsequent incremental backups.
- **[Incremental backup](../glossary.md#backup)**: Captures only changes (inserts, updates, deletes) since the previous backup. Requires the entire backup chain for restoration.
- **[Backup chain](../glossary.md#backup-chain)**: An ordered sequence starting with a full backup followed by zero or more incremental backups.

## Limitations

Before using backup collections, understand these constraints:

- **Row-oriented tables only**: [Column-oriented tables](table.md#column-oriented-tables) are not supported.
- **One collection per table**: A table can only belong to one backup collection at a time. To back up a table in a different collection, you must first remove it from the existing collection by dropping that collection.
- **Immutable membership**: Once created, the table list in a collection cannot be modified. To add new tables, create a new collection that includes all desired tables.
- **No partial restore**: You cannot restore individual tables from a collection; the entire collection is restored together.
- **External scheduling required**: {{ ydb-short-name }} does not provide built-in backup scheduling. Use external tools like cron for automated backups.

## Architecture

Backup collections use a copy-on-write mechanism combined with [changefeeds](../cdc.md) for efficient incremental backups. This section explains how the components work together.

### How Backup Collections Work

The following diagram illustrates the backup workflow:

```mermaid
block-beta
    columns 5

    CREATE["Create\nCollection"]
    FULL["Full\nBackup"]
    INC1["Incremental\nBackup"]
    INC2["Incremental\nBackup"]
    DROP["Drop\nCollection"]

    CREATE --> FULL
    FULL --> INC1
    INC1 --> INC2
    INC2 --> DROP

    style CREATE fill:#1976D2,color:#fff
    style FULL fill:#1565C0,color:#fff
    style INC1 fill:#42A5F5,color:#fff
    style INC2 fill:#64B5F6,color:#000
    style DROP fill:#E91E63,color:#fff
```

**Collection creation** defines which tables to include and creates the schema object. This is a fast, metadata-only operation.

**Full backup** creates a consistent snapshot of all tables in the collection. Key characteristics:

- Uses a **global snapshot** that ensures referential integrity across all tables in the collection.
- Creates **changefeeds** on each table to track subsequent modifications.
- Uses **copy-on-write**: The backup is created quickly by referencing existing data; actual data copying occurs only when source data is modified.

**Incremental backup** captures all changes since the previous backup:

- Uses a **distributed transaction** to read changefeeds from all tables at a consistent point, ensuring referential integrity across the collection.
- Reads accumulated changes from the changefeeds created during the full backup.
- Records all modifications: inserts, updates, and deletes (as tombstone records).
- Compacts changefeed data into incremental backup tables.

{% note warning %}

Schema changes (ALTER TABLE) to tables in a backup collection are not tracked by incremental backups. If you need to modify the schema of a backed-up table, create a new full backup after the schema change to ensure the backup chain reflects the new structure.

{% endnote %}

{% note info %}

Changefeeds created during full backup are automatically removed when the backup collection is dropped. They cannot be manually removed or reused for other purposes while the collection exists.

{% endnote %}

### Storage

Backup collections are stored within the {{ ydb-short-name }} cluster in a dedicated directory structure:

```text
/Root/database/.backups/collections/
├── my_collection/
│   ├── 20250821141425Z_full/
│   │   ├── table_1/
│   │   └── table_2/
│   └── 20250821151519Z_incremental/
│       ├── table_1/
│       └── table_2/
```

{% note info %}

The `.backups` directory is created automatically when the first backup collection is created. Do not create this directory manually. Once it exists, you can manage backup tables within it (for example, when exporting or importing backups).

{% endnote %}

#### Cluster Storage

By default, backups are stored within the cluster. Cluster-stored backups are designed for recovery from **logical errors** such as accidental `DROP TABLE`, `TRUNCATE TABLE`, or erroneous data modifications. Benefits include:

- Fast backup and restore operations
- Integrated security mechanisms
- No external infrastructure required

{% note warning %}

Cluster-stored backups share the same fault domain as the data they protect. If the cluster experiences a failure that exceeds its fault tolerance (such as total cluster loss or catastrophic data center events), both the data and backups may be lost. For protection against such scenarios, use external storage.

{% endnote %}

#### External Storage

For **disaster recovery** protection against cluster-wide failures, regularly export backup collections to external storage (S3-compatible storage or filesystem) using [export/import operations](../../reference/ydb-cli/export-import/index.md).

To export backups to external storage, use the {{ ydb-short-name }} CLI:

- `ydb export s3` for S3-compatible storage
- `ydb tools dump` for filesystem storage

Each backup in the chain must be exported separately. Preserve the chain order during export/import to ensure successful restoration.

### Background Operations

All backup and restore operations run asynchronously, allowing normal database operations to continue. Monitor progress using `ydb operation list incbackup`.

## Restoring from Backups

Restoration recovers data from a backup chain to a specified point in time.

### Restore Workflow

1. **Import from external storage** (if applicable): If backups were exported, import the full backup and all incremental backups up to the desired restore point.

2. **Execute restore**: Run `RESTORE collection_name` to restore all tables from the backup collection. The system applies the full backup and all incremental backups in sequence to reach the most recent backup point.

{% note warning %}

The restore operation overwrites existing tables with the same names. Back up or rename existing tables before restoring if you need to preserve their current data.

{% endnote %}

The restore operation maintains transactional consistency across all tables in the collection.

{% note info %}

During the restore operation, target tables are placed in an `EPathStateIncomingIncrementalRestore` state. Partially restored data may be visible to workloads while the operation is in progress. Plan restore operations during maintenance windows if your application cannot handle tables in this state.

{% endnote %}

## See Also

- [Backup and Recovery guide](../../devops/backup-and-recovery/index.md#backup-collections): Practical operations guide
- [Recipes and examples](../../recipes/backup-collections/index.md): Common scenarios and examples
- YQL reference:
  - [CREATE BACKUP COLLECTION](../../yql/reference/syntax/create-backup-collection.md)
  - [BACKUP](../../yql/reference/syntax/backup.md)
  - [RESTORE](../../yql/reference/syntax/restore-backup-collection.md)
  - [DROP BACKUP COLLECTION](../../yql/reference/syntax/drop-backup-collection.md)

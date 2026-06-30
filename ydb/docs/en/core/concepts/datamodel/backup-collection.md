# Backup collection

A backup collection organizes full and incremental backups of selected [row tables](table.md#row-oriented-tables) into manageable chains. It allows you to **restore data to the state of the latest backup in the chain**, protecting against accidental data loss — erroneous deletions or modifications.

{% note info %}

For practical instructions on creating and managing backup collections, see the [Backup collections](../../devops/backup-and-recovery/index.md#backup-collections) section of the Backup and Restore document.

{% endnote %}

## Overview

Backup collections solve typical backup tasks for production workloads:

- **Storage efficiency**: Incremental backups capture only changes since the previous backup, significantly reducing storage requirements compared to multiple full backups.
- **Consistent restore**: All tables in the collection are created from a single global snapshot, ensuring referential integrity between tables during restore.
- **Chain restore**: Restore to the state of the **latest** backup in the current chain — see [`RESTORE`](../../yql/reference/syntax/restore-backup-collection.md).

For comparison with other backup methods (export/import, dump/restore), see [Backup concepts](../backup.md).

## Key concepts

These terms are essential for understanding backup collections. For detailed definitions, see the [glossary](../glossary.md#backup-collection).

- **[Full backup](../glossary.md#backup)**: A complete snapshot of all data in the collection at a specific point in time. Serves as the basis for subsequent incremental backups.
- **[Incremental backup](../glossary.md#backup)**: Captures only changes (inserts, updates, deletes) since the previous backup. Requires the entire backup chain for restore.
- **[Backup chain](../glossary.md#backup-chain)**: An ordered sequence starting with a full backup, followed by zero or more incremental backups.

## Limitations {#limitations}

Before using backup collections, consider these limitations:

- **Row tables only**: [Column tables](table.md#column-oriented-tables) are not supported.
- **One collection per table**: A table can belong to only one backup collection. To include a table in another collection, run [`DROP BACKUP COLLECTION`](../../yql/reference/syntax/drop-backup-collection.md) for the current collection and create a new one with the desired set of tables.
- **Immutable composition**: After creation, the list of tables in a collection cannot be changed. To add new tables, create a new collection with all the required tables.
- **No partial restore**: You cannot restore individual tables from a collection; the entire collection is restored.
- **External scheduling**: {{ ydb-short-name }} does not provide built-in backup scheduling. Use external tools such as cron for automation.

## Architecture

Backup collections use a copy-on-write mechanism combined with [change streams](../cdc.md) for efficient incremental backups. This section explains how the components work together.

### How backup collections work

The following diagram illustrates the backup process:


```mermaid
block-beta
    columns 5

    CREATE["Создание\nколлекции"]
    FULL["Полная\nкопия"]
    INC1["Инкрементальная\nкопия"]
    INC2["Инкрементальная\nкопия"]
    DROP["Удаление\nколлекции"]

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


**Collection creation** defines which tables to include and creates a schema object. This is a fast, metadata-only operation.

**Full backup** creates a consistent snapshot of all tables in the collection. Key characteristics:

- Uses a **global snapshot** that ensures consistency across all tables in the collection.
- Creates **change streams** for each table to track subsequent modifications.
- Uses **copy-on-write**: The backup is created quickly by referencing existing data; actual data copying occurs only when the source data is modified.

**Incremental backup** captures all changes since the previous backup:

- Uses a **distributed transaction** to read change streams from all tables at a consistent point, ensuring referential integrity across the entire collection.
- Reads accumulated changes from change streams created during the full backup and accumulating changes **since the previous backup (full or incremental)**.
- Writes all modifications: inserts, updates, and deletes (including as deletion records).
- Saves change stream data to incremental backup tables.

{% note warning %}

Schema changes (ALTER TABLE) of tables in a backup collection are not tracked by incremental backups. If you need to change the schema of a table with a backup, create a new full backup after the schema change so that the backup chain reflects the new structure.

{% endnote %}

{% note info %}

Change streams created during a full backup are automatically deleted when the backup collection is deleted. They cannot be manually deleted or used for other purposes while the collection exists.

{% endnote %}

### Storage

Backup collections are stored in the {{ ydb-short-name }} cluster in a dedicated directory structure:


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

The `.backups` directory is created automatically when the first backup collection is created and is managed by the system. After its creation, you can manage backup tables inside it (for example, when exporting or importing backups).

{% endnote %}

#### Cluster storage

By default, backups are stored in the cluster. Backups in the cluster are intended for recovery from **logical errors**, such as accidental `DROP TABLE`, `TRUNCATE TABLE`, or erroneous data changes. Advantages:

- Fast backup and restore operations.
- Integrated security mechanisms.
- No external infrastructure required.

{% note warning %}

Backups in the cluster are in the same failure zone as the protected data. If the cluster fails beyond its fault tolerance (for example, complete cluster loss or catastrophic events in the data center), both the data and backups may be lost. To protect against such scenarios, use external storage.

{% endnote %}

#### External storage {#external-storage}

For **disaster recovery** and protection against entire cluster failures, regularly export backup collections to external storage (S3-compatible storage or file system) using [export/import operations](../../reference/ydb-cli/export-import/index.md).

To export backups to external storage, use the {{ ydb-short-name }} CLI:

- [`ydb export s3`](../../reference/ydb-cli/export-import/export-s3.md) for S3-compatible storage.
- [`ydb export nfs`](../../reference/ydb-cli/export-import/export-nfs.md) for NFS on cluster hosts.
- [`ydb tools dump`](../../reference/ydb-cli/export-import/tools-dump.md) for a file system on a local computer.

Each backup in the chain must be exported separately. Maintain the chain order during export/import for successful recovery.

### Background operations

All backup and restore operations are performed asynchronously, allowing you to continue normal database operations. Track progress using [`ydb operation list incbackup`](../../reference/ydb-cli/operation-list.md).

## Restore from backups

Restore returns data to the state of the latest backup in the chain that is in the cluster. To restore to an earlier state, import only the required chain prefix from external storage — see [Import and restore](../../recipes/backup-collections/importing-and-restoring.md).

### Restore process

1. **Import from external storage** (if necessary): If backups were exported, import the full backup and all incremental backups up to the desired restore point.
2. **Perform restore**: Run `RESTORE collection_name` to restore all tables from the backup collection. The system applies the full backup and all incremental backups sequentially to reach the latest backup point.

{% note warning %}

The restore operation will fail if at least one of the tables being restored already exists at the same path. Rename or delete conflicting tables before restoring.

{% endnote %}

All tables in the collection are restored to the same point in time: data from different tables will be mutually consistent.

{% note info %}

During a restore operation, the target tables are unavailable for modifications. Partially restored data may be visible to read workloads. Plan the restore for a maintenance window or disable application access to the affected tables until the operation completes.

{% endnote %}

## See also

- [Backup concepts](../backup.md): Overview of all backup approaches in {{ ydb-short-name }}
- [Backup and restore: backup collections](../../devops/backup-and-recovery/index.md#backup-collections): step-by-step scenarios and CLI commands
- [Recipes and examples](../../recipes/backup-collections/index.md): Typical scenarios and examples
- YQL reference:

  - [CREATE BACKUP COLLECTION](../../yql/reference/syntax/create-backup-collection.md)
  - [BACKUP](../../yql/reference/syntax/backup.md)
  - [RESTORE](../../yql/reference/syntax/restore-backup-collection.md)
  - [DROP BACKUP COLLECTION](../../yql/reference/syntax/drop-backup-collection.md)

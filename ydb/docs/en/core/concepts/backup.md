# Backup concepts

{{ ydb-short-name }} ensures data safety during hardware failures through [replication and fault tolerance](topology.md#cluster-config). However, replication does not protect against **logical errors**: an accidental `DROP TABLE`, a mistaken mass `UPDATE`, or `DELETE` will be replicated to all replicas. To protect against such scenarios, **backup** is required — a separate copy of data that can be restored from.

## Full backup {#full-backup}

A full backup is a snapshot of table data at a specific point in time. {{ ydb-short-name }} provides several ways to create full backups, from simple to more functional.

### Copying tables within a cluster {#copy-table}

The simplest way is to create a copy of a table (or several tables) within the same cluster using the [`{{ ydb-cli }} tools copy`](../reference/ydb-cli/tools-copy.md) command. The copy is created atomically from a consistent snapshot and uses a copy-on-write mechanism, so the operation is fast.

Suitable for:

- Quickly creating a safety copy before a dangerous operation.
- Cloning data for testing.

{% note warning %}

The copy is stored in the same cluster as the original data. It protects against logical errors but not against cluster loss.

{% endnote %}

### Dump to file system {#dump}

The [`{{ ydb-cli }} tools dump`](../reference/ydb-cli/export-import/tools-dump.md) and [`{{ ydb-cli }} tools restore`](../reference/ydb-cli/export-import/tools-restore.md) commands allow you to dump data to a local file system and restore it back.

Suitable for:

- Local development and testing.
- Small databases.
- Creating a copy on a separate medium.

### Export to S3-compatible storage {#s3}

The [`{{ ydb-cli }} export s3`](../reference/ydb-cli/export-import/export-s3.md) and [`{{ ydb-cli }} import s3`](../reference/ydb-cli/export-import/import-s3.md) commands allow you to export and import data to an external S3-compatible storage.

Suitable for:

- Disaster recovery (data is stored outside the cluster).
- Data migration between clusters.
- Long-term archiving.

### Export to NFS {#nfs}

The [`{{ ydb-cli }} export nfs`](../reference/ydb-cli/export-import/export-nfs.md) and [`{{ ydb-cli }} import nfs`](../reference/ydb-cli/export-import/import-nfs.md) commands allow you to export and import data to a network file system (NFS) mounted on all hosts of the {{ ydb-short-name }} cluster. Unlike [dump to file system](#dump), the export is performed server-side and does not require data transfer via CLI.

Suitable for:

- Disaster recovery (data is stored outside the cluster).
- Data migration between clusters.
- Long-term archiving.

For more details on configuring NFS for backup and recovery, see the recipe [Backup and recovery via NFS](../recipes/nfs-backup/nfs-backup.md).

## Incremental backup {#incremental-backup}

When working with large tables, repeatedly creating full backups can be too expensive. Incremental backup solves this problem: after an initial full copy, each subsequent increment captures only the changes (inserts, updates, deletes) that occurred since the previous backup.

Incremental backups are organized into a **chain**:


```text
Full backup → Increment₁ → Increment₂ → ... → Incrementₙ
```


To restore, the entire chain is needed: first the full copy is applied, then all increments sequentially. Restoration is performed to the state at the time of the last increment in the chain.

Incremental backup is implemented using [backup collections](datamodel/backup-collection.md).

{% note info %}

Currently, only [row tables](datamodel/table.md#row-oriented-tables) are supported.

{% endnote %}

## Comparison of approaches {#comparison}

#|
|| **Method** | **Storage location** | **Increments** | **Scenarios** ||
|| [Copying tables within a cluster](#copy-table) | In the cluster | No | Quick copy before a dangerous operation ||
|| [Dump to file system](#dump) | File system | No | Development, testing, small databases ||
|| [Export to S3-compatible storage](#s3) | S3-compatible storage | No | Disaster recovery, migration, archiving ||
|| [Export to NFS](#nfs) | Network file system (NFS) | No | Disaster recovery, migration, archiving ||
|| [Incremental backup](#incremental-backup) | In the cluster ([exported](datamodel/backup-collection.md#external-storage) to S3 or file system) | Yes | Regular backups of large production databases ||
|#

## See also

- [Backup collections](datamodel/backup-collection.md) — architecture and limitations
- [Backup and recovery](../devops/backup-and-recovery/index.md) — practical guide
- [Export/import reference](../reference/ydb-cli/export-import/index.md) — CLI commands
- YQL reference:

  - [`CREATE BACKUP COLLECTION`](../yql/reference/syntax/create-backup-collection.md)
  - [`BACKUP`](../yql/reference/syntax/backup.md)
  - [`RESTORE`](../yql/reference/syntax/restore-backup-collection.md)
  - [`DROP BACKUP COLLECTION`](../yql/reference/syntax/drop-backup-collection.md)

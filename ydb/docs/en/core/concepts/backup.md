# Backup concepts

{{ ydb-short-name }} ensures data safety during hardware failures through [replication and fault tolerance](topology.md#cluster-config). However, replication does not protect against **logical errors**: an accidental `DROP TABLE`, an erroneous mass `UPDATE`, or `DELETE` will be replicated to all replicas. To protect against such scenarios, **backup** is required — a separate copy of data that can be restored from.

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

The [`{{ ydb-cli }} export nfs`](../reference/ydb-cli/export-import/export-nfs.md) and [`{{ ydb-cli }} import nfs`](../reference/ydb-cli/export-import/import-nfs.md) commands allow you to export and import data to a network file system (NFS) mounted on all hosts of the {{ ydb-short-name }} cluster. Unlike [dump to file system](#dump), the export is performed on the server side and does not require data transfer via CLI.

Suitable for:

- Disaster recovery (data is stored outside the cluster).
- Data migration between clusters.
- Long-term archiving.

For more details on configuring NFS for backup and recovery, see the recipe [Backup and recovery via NFS](../recipes/nfs-backup/nfs-backup.md).

## Incremental backup {#incremental-backup}

When working with large tables, repeatedly creating full backups can be too costly. Incremental backup solves this problem: after an initial full copy, each subsequent increment captures only the changes (inserts, updates, deletes) that occurred since the previous backup.

Incremental backups are organized into a **chain**:


```text
Full copy → Increment₁ → Increment₂ → ... → Incrementₙ
```


For recovery, the entire chain is required: first, the full copy is applied, then all increments sequentially. Recovery is performed to the state at the time of the last increment in the chain.

Incremental backup is implemented using [backup collections](datamodel/backup-collection.md).

{% note info %}

Currently, only [row tables](datamodel/table.md#row-oriented-tables) are supported.

{% endnote %}

## Backup of system tablets {#system-tablet-backup}

{% note info %}

Currently, only cluster-level system tablets are supported for backup. Database system tablet backup is not supported.

{% endnote %}

The system tablet backup mechanism provides incremental copying of cluster metadata — such as [Hive](glossary.md#hive), [BSController](glossary.md#ds-controller), and [SchemeShard](glossary.md#scheme-shard) — to the local file system of the cluster hosts.

This mechanism is used to restore cluster metadata when [recovery from database backups](#full-backup) is technically possible but unsuitable in terms of time or workload. A typical scenario is when the aggregate volume of databases in the cluster is large due to their number, the size of individual databases, or both; a full `import`/`restore` of all data to a new cluster in this case leads to prolonged downtime. In this scenario, you can restore system tablets and return the cluster to an operational state without performing a mass restore of user data on a new cluster.

If the database volume allows standard recovery, use [export/import](#s3) or [dump/restore](#dump) first. System tablet backup should be used as a specialized mechanism when you need to restore cluster metadata specifically and reduce the scope of recovery operations.

{% note info %}

For practical instructions on enabling backup and recovery, see [System tablet backup recipes](../recipes/system-tablet-backup/index.md).

{% endnote %}

{% note warning %}

Backups of different system tablets are created independently of each other and are not coordinated. After recovery, the state of the tablets may be inconsistent, which can negatively affect cluster operation.

{% endnote %}

### How it works {#system-tablet-backup-how-it-works}

Backup consists of two components:

- **Snapshot** — on each run, the tablet scans all its tables and writes its full state to the backup copy, including the data schema. Scanning is performed from a snapshot and does not block tablet operation.
- **Changelog** — on each change to data or schema, the tablet asynchronously writes the change to the log in parallel with writing to distributed storage. When the log size exceeds the snapshot size, the tablet automatically creates a new snapshot.

{% note warning %}

Because writes are asynchronous, recent changes that did not make it into the backup before a failure may be lost.

{% endnote %}

Backups are created **locally on the host where the tablet is currently running**. Therefore, the most up-to-date copy is on the host where the tablet was running immediately before the failure.

The number of backup copies stored on a host is limited in the [configuration](../reference/configuration/system_tablet_backup_config.md). After a snapshot is successfully taken, the oldest copy is automatically deleted when the limit is exceeded. Incomplete copies (without a fully written snapshot) are deleted when a new backup copy is created.

## Comparison of approaches {#comparison}

#|
|| **Method** | **Storage location** | **Increments** | **Scenarios** ||
|| [Copying tables within a cluster](#copy-table) | In the cluster | No | Quick copy before a dangerous operation ||
|| [Dump to file system](#dump) | File system | No | Development, testing, small databases ||
|| [Export to S3-compatible storage](#s3) | S3-compatible storage | No | Disaster recovery, migration, archiving ||
|| [Export to NFS](#nfs) | Network file system (NFS) | No | Disaster recovery, migration, archiving ||
|| [Incremental backup](#incremental-backup) | In the cluster ([exported](datamodel/backup-collection.md#external-storage) to S3 or file system) | Yes | Regular backups of large production databases ||
|| [Backup of system tablets](#system-tablet-backup) | Local file system of cluster hosts | Yes | Restoring cluster metadata in emergency situations ||
|#

## See also

- [Backup and recovery](../devops/backup-and-recovery/index.md) — practical guide
- [Backup collections](datamodel/backup-collection.md) — architecture and limitations
- [System tablet backup recipes](../recipes/system-tablet-backup/index.md) — enabling and recovery
- [Backup and recovery](../devops/backup-and-recovery/index.md) — practical guide
- [Export/import reference](../reference/ydb-cli/export-import/index.md) — CLI commands
- YQL reference:

  - [`CREATE BACKUP COLLECTION`](../yql/reference/syntax/create-backup-collection.md)
  - [`BACKUP`](../yql/reference/syntax/backup.md)
  - [`RESTORE`](../yql/reference/syntax/restore-backup-collection.md)
  - [`DROP BACKUP COLLECTION`](../yql/reference/syntax/drop-backup-collection.md)

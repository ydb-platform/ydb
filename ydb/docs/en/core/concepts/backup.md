# Backup Concepts

{{ ydb-short-name }} ensures data durability against hardware failures through [replication and fault tolerance](topology.md#cluster-config). However, replication does not protect against **logical errors**: an accidental `DROP TABLE`, an erroneous bulk `UPDATE`, or `DELETE` will be replicated to all copies. To protect against such scenarios, you need **backups** — separate copies of data that you can restore from.

## Full backups {#full-backup}

A full backup is a snapshot of table data at a specific point in time. {{ ydb-short-name }} provides several ways to create full backups, from simple to more feature-rich.

### Copying tables within the cluster {#copy-table}

The simplest approach is to create a copy of one or more tables within the same cluster using [`{{ ydb-cli }} tools copy`](../reference/ydb-cli/tools-copy.md). The copy is created atomically from a consistent snapshot using a copy-on-write mechanism, making the operation fast.

Suitable for:

- creating a quick safety copy before a risky operation
- cloning data for testing

{% note warning %}

The copy is stored in the same cluster as the source data. It protects against logical errors but not against cluster loss.

{% endnote %}

### Dump to filesystem {#dump}

The [`{{ ydb-cli }} tools dump`](../reference/ydb-cli/export-import/tools-dump.md) and [`{{ ydb-cli }} tools restore`](../reference/ydb-cli/export-import/tools-restore.md) commands allow you to export data to the local filesystem and restore it back.

Suitable for:

- local development and testing
- small databases
- creating a copy on a separate storage medium

### Export to S3-compatible storage {#s3}

The [`{{ ydb-cli }} export s3`](../reference/ydb-cli/export-import/export-s3.md) and [`{{ ydb-cli }} import s3`](../reference/ydb-cli/export-import/import-s3.md) commands allow you to export and import data to external S3-compatible storage.

Suitable for:

- disaster recovery (data is stored outside the cluster)
- data migration between clusters
- long-term archival

## Incremental backups {#incremental-backup}

For large tables, repeatedly creating full backups can be too expensive. Incremental backups solve this: after an initial full backup, each subsequent increment captures only the changes (inserts, updates, deletes) that occurred since the previous backup.

Incremental backups are organized into a **chain**:

```text
Full backup → Increment₁ → Increment₂ → ... → Incrementₙ
```

Restoration requires the entire chain: the full backup is applied first, then all increments in sequence. Recovery restores data to the state at the time of the last increment in the chain.

Incremental backups are implemented using [backup collections](#backup-collections).

{% note info %}

Currently only [row-oriented tables](datamodel/table.md#row-oriented-tables) are supported.

{% endnote %}

## Backup collections {#backup-collections}

For details on backup collections, see [Backup collections](datamodel/backup-collection.md).

## Comparison {#comparison}

#|
|| **Method** | **Storage location** | **Incremental** | **Use cases** ||
|| [Copying tables within the cluster](#copy-table) | In cluster | No | Quick copy before a risky operation ||
|| [Dump to filesystem](#dump) | Filesystem | No | Development, testing, small databases ||
|| [Export to S3-compatible storage](#s3) | S3-compatible storage | No | Disaster recovery, migration, archival ||
|| [Incremental backups](#incremental-backup) | In cluster ([exportable](datamodel/backup-collection.md#external-storage) to S3 or filesystem) | Yes | Regular backups of large production databases ||
|#

## See Also

- [Backup collections](datamodel/backup-collection.md) — architecture and limitations
- [Backup and Recovery](../devops/backup-and-recovery.md) — practical operations guide
- [Export/import reference](../reference/ydb-cli/export-import/index.md) — CLI commands
- YQL reference:
  - [`CREATE BACKUP COLLECTION`](../yql/reference/syntax/create-backup-collection.md)
  - [`BACKUP`](../yql/reference/syntax/backup.md)
  - [`RESTORE`](../yql/reference/syntax/restore-backup-collection.md)
  - [`DROP BACKUP COLLECTION`](../yql/reference/syntax/drop-backup-collection.md)

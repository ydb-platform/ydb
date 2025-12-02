# Backup Concepts

This section covers backup concepts and technologies available in {{ ydb-short-name }}.

{{ ydb-short-name }} provides several backup capabilities that can be classified in two ways: by storage type and by backup method.

## Classification by Storage Type {#by-storage}

Backups can be stored in different locations depending on your requirements:

#|
|| **Storage type** | **Backup type** | **Survives cluster failure** | **Use cases** | **How to use** ||
|| S3-compatible storage | Full backups | Yes | Large data migration, archival, production data transfers | [`ydb export s3`](../reference/ydb-cli/export-import/export-s3.md) / [`ydb import s3`](../reference/ydb-cli/export-import/import-s3.md) ||
|| File system | Full backups | Yes | Local development, testing, smaller production environments | [`ydb tools dump`](../reference/ydb-cli/export-import/tools-dump.md) / [`ydb tools restore`](../reference/ydb-cli/export-import/tools-restore.md) ||
|| Cluster storage | Full and incremental backups | No | Production environments with regular backup schedules, large datasets | [Backup collections](datamodel/backup-collection.md) ||
|#

{% note info %}

Cluster-stored backups benefit from {{ ydb-short-name }}'s built-in replication and fault tolerance but do not protect against cluster-wide failures. For disaster recovery, combine cluster-stored backup collections with regular exports to external storage.

{% endnote %}

## Classification by Backup Method {#by-method}

Backups can be created using different methods:

#|
|| **Backup type** | **Storage options** | **Use cases** | **Supported tables** ||
|| Full backup | S3, filesystem, cluster | One-time backups, database migration, testing | All table types ||
|| Incremental backup | Cluster (exportable to S3/filesystem) | Regular backup schedules, large datasets with low change rate | [Row-oriented tables](datamodel/table.md#row-oriented-tables) only ||
|#

Incremental backups are more storage-efficient than multiple full backups, as they capture only changes since the previous backup rather than duplicating all data.

## Backup Collections {#backup-collections}

[Backup collections](datamodel/backup-collection.md) are [schema objects](datamodel/index.md) that organize full and incremental backups in a structured manner, enabling efficient point-in-time recovery for production workloads.

Key characteristics:

- **Cluster storage with export capability**: Collections are stored within the cluster and can be exported to external storage using [export/import](../reference/ydb-cli/export-import/index.md) or [dump/restore](../reference/ydb-cli/export-import/tools-dump.md) operations.
- **Consistent snapshots**: All tables in a collection are backed up from the same global snapshot.
- **Incremental support**: After an initial full backup, incremental backups capture only changes.

## See Also

- [Backup collections](datamodel/backup-collection.md): Schema object reference and architecture details
- [Backup and Recovery guide](../devops/backup-and-recovery.md): Practical operations guide
- [Export/import reference](../reference/ydb-cli/export-import/index.md): CLI commands for backup operations
- [CREATE BACKUP COLLECTION](../yql/reference/syntax/create-backup-collection.md): YQL syntax reference
- [BACKUP](../yql/reference/syntax/backup.md): YQL syntax reference
- [RESTORE](../yql/reference/syntax/restore-backup-collection.md): YQL syntax reference

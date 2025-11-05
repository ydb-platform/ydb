# Backup concepts

This section covers backup concepts and technologies available in {{ ydb-short-name }}.

{{ ydb-short-name }} provides several backup capabilities that can be classified in two ways: by storage type and by backup method.

## Classification by storage type {#by-storage}

Backups can be stored in different locations depending on your requirements:

|| Storage type | Backup type | Best for | How to use |
||--------------|-------------|----------|------------|
|| **S3-compatible storage** | Full snapshots of selected objects | Large data migration, archival, production data transfers | ydb export/import - link to [CLI docs](../reference/ydb-cli/export-import/index.md) |
|| **File system** | Full snapshots of selected objects | Local development, testing, smaller production environments | ydb dump/restore - link to [CLI docs](../reference/ydb-cli/export-import/index.md) |
|| **Locally in database** | Full snapshots of selected objects, incremental backups of selected objects | Production environments, large datasets | Requires backup collections - link to [backup collection docs](backup-collections.md) |

## Classification by backup method {#by-method}

Backups can be created using different methods:

|| Storage type | Best for | How to use | Limitations |
||--------------|----------|------------|-------------|
|| **Full snapshot** | Locally in database, s3-compatible storage, file system | One-time backups, database migration, testing, smaller production environments | ydb export/import or ydb dump/restore or link to [CLI docs](../reference/ydb-cli/export-import/index.md) | - |
|| **Incremental backup** | Locally in database, s3-compatible storage*, file system* | Production environments, large datasets, regular backup schedules | Requires backup collections - link to [backup collection docs](backup-collections.md) | Only tables are backed up for now |

\* In order to store incremental backup chain in s3-compatible storage or file system it is required to make a backup collection locally first, and then to back it up its directory using full snapshot backup.

## Backup collections {#backup-collections}

Backup collections provide a way to organize full and incremental backups in a structured manner, enabling efficient point-in-time recovery for production workloads. Collections are stored in a dedicated directory structure within the database, which can be exported to external storage using [export/import](../reference/ydb-cli/export-import/index.md) or dump/restore operations. See [Storage backends](backup-collections.md#storage-backends) for more details.

Learn more:

- [Backup collections concepts](backup-collections.md) - Architecture and concepts.
- [Operations guide](../maintenance/manual/backup-collections.md) - Practical operations.
- [Common recipes](../recipes/backup-collections.md) - Usage examples.

## See also

- [Backup and recovery guide](../devops/backup-and-recovery.md).
- [Export and import reference](../reference/ydb-cli/export-import/index.md).

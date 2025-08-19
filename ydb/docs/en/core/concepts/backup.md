# Backup concepts

This section covers backup concepts and technologies available in {{ ydb-short-name }}.

{{ ydb-short-name }} provides several approaches for creating backups, each designed for different use cases and requirements:

## Export/import

For large-scale data migration and portability scenarios:

- **Use cases**: Large data migration between systems, archival storage, production data transfers
- **Characteristics**: Point-in-time snapshots with flexible format options, optimized for large datasets
- **Storage**: S3-compatible storage

## Backup/restore

For local database backups and development workflows:

- **Use cases**: Local development environments, testing scenarios, smaller production environments, database cloning for local use
- **Characteristics**: Designed for local filesystem operations with moderate data volumes
- **Storage**: Filesystem

## Backup collections

For production workloads requiring incremental backups:

- **Use cases**: Production environments, large datasets, regular backup schedules
- **Characteristics**: Full and incremental backups organized in collections
- **Storage**: Cluster-managed storage with optional export to external systems
- **Operations**: Background operations with progress monitoring via long operations API

Learn more:

- [Export and import reference](../reference/ydb-cli/export-import/index.md) - Export/import operations
- [Backup collections](backup/collections.md) - Concepts, architecture, and when to use
- [CLI tools](../reference/ydb-cli/export-import/backup-collections/index.md) - Command-line tools and detailed usage guide

## Choosing the right approach

| Approach | Best for | Key advantages | Considerations |
|----------|----------|----------------|----------------|
| **Export/import** | Large data migration, archival, production data transfers | Portability between systems, flexible formats, handles large datasets | Full snapshots only |
| **Backup/restore** | Local development, testing, smaller production environments | Local filesystem operations, suitable for moderate data volumes | Full snapshots only, primarily for local use |
| **Backup collections** | Production environments, large datasets | Incremental efficiency, point-in-time recovery | More complex setup, requires planning |

## See also

- [Backup and recovery guide](../devops/backup-and-recovery.md)
- [Export and import reference](../reference/ydb-cli/export-import/index.md)

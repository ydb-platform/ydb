# Backup concepts

This section covers backup concepts and technologies available in {{ ydb-short-name }}.

{{ ydb-short-name }} provides several approaches for creating backups:

## Traditional export/import

For single tables or small datasets, you can use the traditional export/import functionality:

- [Export and import](../reference/ydb-cli/export-import/index.md) - Basic export/import operations
- [Backup and recovery](../devops/backup-and-recovery.md) - Comprehensive backup strategies

## Backup collections (New)

For production workloads and large datasets, use backup collections with incremental backup capabilities:

- [Backup collections](backup/collections.md) - Full and incremental backups organized in collections
- [CLI tools](../reference/ydb-cli/export-import/backup-collections/index.md) - Command-line tools for backup collections

## Choosing the right approach

- **Traditional export/import**: Small databases, one-time backups, simple recovery scenarios
- **Backup collections**: Production environments, large datasets, regular backup schedules, point-in-time recovery requirements

## See also

- [Backup and recovery guide](../devops/backup-and-recovery.md)
- [Export and import reference](../reference/ydb-cli/export-import/index.md)

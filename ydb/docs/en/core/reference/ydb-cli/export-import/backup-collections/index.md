# Backup collections

This section describes backup collections and incremental backup functionality in YDB.

{% note info %}

Backup collections are available starting from YDB vX.Y+ (version TBD).

{% endnote %}

## In this section

- [Overview](overview.md) — Introduction to backup collections
- [SQL API](sql-api.md) — Using SQL commands for backup operations
- [Creating collections](create-collection.md) — How to create and configure backup collections
- [Incremental backups](incremental-backups.md) — Working with incremental backups
- [Restore operations](restore-from-collection.md) — Restoring from backup collections
- [Managing collections](manage-collections.md) — Collection management and maintenance
- [Advanced topics](advanced.md) — Performance, encryption, and optimization
- [Compatibility](compatibility.md) — Format versions and migration
- [Troubleshooting](troubleshooting.md) — Common issues and solutions

## Quick start

For a hands-on introduction, see the [demo cookbook](_includes/demo-cookbook.md) that walks through creating a collection, taking backups, and restoring to different points in time.

## Key concepts

- **Backup collection**: Named set of backups for selected tables
- **Full backup**: Complete snapshot of all data in the collection
- **Incremental backup**: Changes since the previous backup in the chain
- **Backup chain**: Ordered sequence of full and incremental backups
- **Point-in-time recovery (PITR)**: Restore data to any specific point in the backup timeline

## Benefits

- **Storage efficiency**: Incremental backups store only changes, reducing storage requirements
- **Flexible recovery**: Restore to any point in time within the backup chain
- **Automated management**: Built-in retention policies and chain maintenance
- **Multiple storage options**: Cluster storage, filesystem export, or S3-compatible storage

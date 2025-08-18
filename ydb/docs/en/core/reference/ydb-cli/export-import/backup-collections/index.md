# Backup collections

## Quick start

To get started with backup collections:

1. [Create a backup collection](create-collection.md) with your tables
2. [Take incremental backups](incremental-backups.md) regularly  
3. [Restore from collections](restore-from-collection.md) when needed

For troubleshooting common issues, see the [troubleshooting guide](troubleshooting.md).

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

## Getting started

This section describes backup collections and incremental backup functionality in YDB. See the [overview](overview.md) for detailed concepts and usage examples.

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

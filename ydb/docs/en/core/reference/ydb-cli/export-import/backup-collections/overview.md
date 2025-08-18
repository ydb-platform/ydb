# Backup collections overview

This section explains how to use backup collections for full and incremental backups, and how they interact with export/import tools.

{% include [common-options](_includes/common-options.md) %}

## What a collection contains

- A chain of backups (one full, zero or more incrementals)
- Metadata (e.g., metadata.json), schema mapping (mapping.json)
- Integrity info (checksums, possibly signatures)

## Supported backends

- **Cluster storage** (SQL: STORAGE='cluster') — Current implementation
- **Export/import to filesystem or S3** using CLI tools (see [includes](_includes/fs-examples.md))

## Security and integrity

- See [encryption and integrity docs](_includes/integrity-and-verification.md); verify backups before restore in critical workflows.
- Encryption support via KMS integration (see [Advanced topics](advanced.md))

## Limitations

- Respect chain order. Do not delete full backups that have dependent incrementals.
- Retention policies must preserve chain validity.
- Maximum chain length recommendations (see [Managing collections](manage-collections.md))

## Next steps

- [Create your first backup collection](create-collection.md)
- [Learn about incremental backups](incremental-backups.md)
- [Explore the SQL API](sql-api.md)

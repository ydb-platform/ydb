# SQL API: Backup collections

This section provides guidance for using SQL commands with [backup collections](concepts.md). For complete syntax reference, see [Backup collection commands](../../../../yql/reference/syntax/backup-collections.md) in the YQL reference.

## Quick reference

The main SQL commands for backup collections are:

- `CREATE BACKUP COLLECTION` - Creates a new backup collection.
- `BACKUP` - Creates a backup (full or incremental).  
- `DROP BACKUP COLLECTION` - Removes a collection and all backups.

For detailed syntax, parameters, and examples, refer to the [YQL syntax reference](../../../../yql/reference/syntax/backup-collections.md).

## Using SQL commands with backup collections

### Connection and execution

Execute backup collection commands through any SQL interface that supports YQL:

- **YDB CLI**: Use `ydb yql` command
- **SDK connections**: Execute as standard SQL queries
- **Web UI**: Run commands in the query editor

### Best practices for SQL operations

- **Use quoted identifiers**: Always quote collection names with backticks
- **Specify absolute paths**: Use full table paths starting with `/Root/`
- **Monitor operations**: Check operation status using [operation list](../../operation-list.md) commands
- **Plan retention**: Consider backup chain dependencies before deletion

## Query backup information

Browse collections through the database schema:

```bash
# List all collections
ydb scheme ls .backups/collections/

# View specific collection structure  
ydb scheme ls .backups/collections/shop_backups/
```

For monitoring backup operations, use the [operation list](../../operation-list.md) command:

```bash
# Monitor backup operations
ydb operation list incbackup
```

## Next steps

- [Complete YQL syntax reference for backup collections](../../../../yql/reference/syntax/backup-collections.md).
- [Learn about backup collection concepts](concepts.md).
- [Explore all operations and management tasks](operations.md).

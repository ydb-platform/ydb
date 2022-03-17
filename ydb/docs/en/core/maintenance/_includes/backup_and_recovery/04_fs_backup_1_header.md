## Backups to the file system {#filesystem_backup}

Saving the structure of the `backup` directory in the `$YDB_DB_PATH` database to the `my_backup_of_basic_example` directory in the file system.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools dump -p $YDB_DB_PATH/backup -o my_backup_of_basic_example/
```

For each directory in the database, a file system directory is created. For each table, a directory is created in the file system to place the structure description file in. Table data is saved to one or more `CSV` files, one file line per table row.

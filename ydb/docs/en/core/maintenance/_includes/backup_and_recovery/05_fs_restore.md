## Restoring data from backups in the file system {#filesystem_restore}

The command below creates directories and tables from the backup saved in the `my_backup_of_basic_example` directory and uploads data to them.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools restore  -p $YDB_DB_PATH/backup/restored -i my_backup_of_basic_example/
```

### Checking backups

The `{{ ydb-cli }} tools restore` command run with the `--dry-run` option checks if a backup contains all DB tables and if all table structures are the same.

The command below checks that all tables saved in the `my_backup_of_basic_example` directory exist in the `$YDB_DB_PATH` database and their structure (column contents and their order, column data types, and primary key contents) is identical.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools restore  -p $YDB_DB_PATH/restored_basic_example -i my_backup_of_basic_example/ --dry-run
```


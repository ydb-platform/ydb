### Example of listing the contents of a directory with a backup

```
tree my_backup_of_basic_example/
my_backup_of_basic_example/
├── episodes
│   ├── data_00.csv
│   └── scheme.pb
├── seasons
│   ├── data_00.csv
│   └── scheme.pb
└── series
    ├── data_00.csv
    └── scheme.pb

3 directories, 6 files
```

The structure of each table is saved in a file named `scheme.pb`. For example, the `episodes/scheme.pb` file stores the schema of the `episodes` table. The data of each table is saved in one or more files named like `data_xx.csv`, where xx is the file's sequence number. The name of the first file is `data_00.csv`. The file size limit is 100 MB.

### Saving table schemas

Running the `{{ ydb-cli }} tools dump` command with the `--scheme-only` option only saves table schemas. The command below saves all directories and files with the table structure from the `examples` directory in the `$YDB_DB_PATH` database to the `my_backup_of_basic_example` directory. No files with table data are created.

```
{{ ydb-cli }} -e $YDB_ENDPOINT -d $YDB_DB_PATH tools dump -p $YDB_DB_PATH/examples -o my_backup_of_basic_example/ --scheme-only
```


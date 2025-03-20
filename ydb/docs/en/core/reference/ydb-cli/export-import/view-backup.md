# View Restoration from Backups: Understanding Reference Changes

When restoring views from a backup, it's important to understand that the view's underlying query may be automatically modified to maintain proper object references. This occurs because the backup and restoration process is designed to be "closed" - meaning all schema objects' locations become relative to the backup root (specified by the `--path` option in the [ydb tools dump](./tools-dump.md#schema-objects) command), and all references are treated relative to this backup root. When such a "closed" backup is restored, views will reference the newly restored tables rather than the previously existing tables in the target environment, preserving the relative positioning between views and their referenced objects as they existed at backup time.

## Examples

### Restoring database root to the same path

Let's consider the following scenario.

1. A view is created with the query:

    ```sql
    CREATE VIEW root_view WITH security_invoker = TRUE AS
        SELECT * FROM root_table;
    ```

2. Database is backed up:

    ```bash
    ydb tools dump --path . --output ./my_backup
    ```

3. Database is cleared:

    ```bash
    ydb scheme rmdir --force --recursive .
    ```

4. Database is restored:

    ```bash
    ydb tools dump --path . --input ./my_backup
    ```

As the result of the steps described above, the `root_view` is restored and it reads from the `root_table`:

```bash
ydb sql --script 'select * from root_view' --explain
```

In the output of the executed command we see: `TableFullScan (Table: root_table, ...`

### Restoring database root to a subfolder

- step 1:

    ```sql
    CREATE VIEW my_view WITH security_invoker = TRUE AS
        SELECT * FROM my_table;
    ```

- step 2:

    ```bash
    ydb tools dump --path . --output ./my_backup
    ```

- step 3:

    ```bash
    ydb tools restore --path a/b/c --input ./my_backup
    ```

The restored view `a/b/c/my_view` reads from the restored table `a/b/c/my_table`:

```bash
ydb sql --script 'select * from `a/b/c/my_view`' --explain
```

In the output of the executed command we see: `TableFullScan (Table: a/b/c/my_table, ...`

### Restoring subfolder to database root

- repeat steps from 1 to 3 of the previous scenario [{#T}](#restoring-database-root-to-a-subfolder).
- create a backup of the `a/b/c` subfolder: `ydb tools dump --path a/b/c --output ./subfolder_backup`
- clear the database: `ydb scheme rmdir`
- restore the subfolder backup to the database root: `ydb tools restore --path . --input ./subfolder_backup`
- observe that the restored view `my_view` references the `my_table` located at the root of the database: `ydb sql --explain`

### Restoring database root to a different database root

Let's consider the following scenario.

1. A view is created with the query:

    ```sql
    CREATE VIEW root_view WITH security_invoker = TRUE AS
        SELECT * FROM root_table;
    ```

2. Database is backed up:

    ```bash
    ydb --endpoint <endpoint> --database /my_database tools dump --path . --output ./my_backup
    ```

3. Database is cleared:

    ```bash
    ydb --endpoint <endpoint> --database /my_database scheme rmdir --force --recursive .
    ```

4. Database backup is restored to a different database:

    ```bash
    ydb --endpoint <endpoint> --database /restored_database tools dump --path . --input ./my_backup
    ```

As the result of the steps described above, the `root_view` is restored and it reads from the `root_table` located in the `/restored_database`:

```bash
ydb --endpoint <endpoint> --database /restored_database sql --script 'select * from root_view' --explain
```

In the output of the executed command we see: `TableFullScan (Table: root_table, ...`

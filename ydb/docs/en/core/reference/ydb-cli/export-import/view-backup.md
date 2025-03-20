# View restoration from backups: understanding reference changes

When restoring [views](../../../concepts/datamodel/view.md) from a backup, it's important to understand that the view's underlying query may be automatically modified to maintain proper object references. This occurs because the backup and restoration process is designed to be "closed" - meaning:

- schema objects' locations are considered relative to the backup root (specified by the `--path` option in the [ydb tools dump](./tools-dump.md#schema-objects) command)
- references are treated relative to the backup root

When such a "closed" backup is restored, views will reference the newly restored tables rather than the previously existing tables in the target environment, preserving the relative positioning between views and their referenced objects as they existed at backup time.

## Examples

### Restoring database root to the same path

Let's consider the following scenario:

1. A view is created by the query:

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

As the result of the steps described above, the view `root_view` is restored and reads from the table `root_table`:

```bash
ydb sql --script 'select * from root_view' --explain
```

In the output of the executed command we see: `TableFullScan (Table: root_table, ...`

### Restoring database root to a subfolder

Let's consider the following scenario:

1. A view is created by the query:

    ```sql
    CREATE VIEW my_view WITH security_invoker = TRUE AS
        SELECT * FROM my_table;
    ```

2. Database is backed up:

    ```bash
    ydb tools dump --path . --output ./my_backup
    ```

3. Database is restored to a subfolder `a/b/c`:

    ```bash
    ydb tools restore --path a/b/c --input ./my_backup
    ```

As the result of the steps described above, the view `a/b/c/my_view` is restored and reads from the table `a/b/c/my_table`:

```bash
ydb sql --script 'select * from `a/b/c/my_view`' --explain
```

In the output of the executed command we see: `TableFullScan (Table: a/b/c/my_table, ...`

### Restoring subfolder to the database root

Let's consider the following scenario:

1. Steps 1 to 3 of the previous scenario [{#T}](#restoring-database-root-to-a-subfolder) are repeated.
2. Subfolder `a/b/c` of the database is backed up:

    ```bash
    ydb tools dump --path a/b/c --output ./subfolder_backup
    ```

3. Database is cleared:

    ```bash
    ydb scheme rmdir --force --recursive .
    ```

4. Subfolder backup is restored to the root of the database:

    ```bash
    ydb tools restore --path . --input ./subfolder_backup
    ```

As the result of the steps described above, the view `my_view` is restored and reads from the table `my_table`:

```bash
ydb sql --script 'select * from my_view' --explain
```

In the output of the executed command we see: `TableFullScan (Table: my_table, ...`

### Restoring database root to the root of a different database

Let's consider the following scenario:

1. A view is created by the query:

    ```sql
    CREATE VIEW root_view WITH security_invoker = TRUE AS
        SELECT * FROM root_table;
    ```

2. Database is backed up:

    ```bash
    ydb --endpoint <endpoint> --database /my_database tools dump --path . --output ./my_backup
    ```

    Note the `--database /my_database` in the connection string.

3. Database backup is restored to a different database:

    ```bash
    ydb --endpoint <endpoint> --database /restored_database tools dump --path . --input ./my_backup
    ```

    Note the `--database /restored_database` in the connection string.

As the result of the steps described above, the `root_view` is restored and it reads from the `root_table` located in the `/restored_database`:

```bash
ydb --endpoint <endpoint> --database /restored_database sql --script 'select * from root_view' --explain
```

In the output of the executed command we see: `TableFullScan (Table: root_table, ...`

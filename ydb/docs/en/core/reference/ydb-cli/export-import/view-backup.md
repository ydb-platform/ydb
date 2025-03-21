# Considerations for restoring views from backups

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

2. The database is backed up:

    ```bash
    ydb tools dump --path . --output ./my_backup
    ```

3. The database is cleared:

    ```bash
    ydb scheme rmdir --force --recursive .
    ```

4. The database is restored:

    ```bash
    ydb tools dump --path . --input ./my_backup
    ```

As the result of the steps described above, the `root_view` view is restored and selects from the `root_table` table:

```bash
ydb sql --script 'select * from root_view' --explain
```

The output of the command includes the following line: `TableFullScan (Table: root_table, ...`

### Restoring database root to a subfolder

Let's consider the following scenario:

1. A view is created using the following query:

    ```sql
    CREATE VIEW my_view WITH security_invoker = TRUE AS
        SELECT * FROM my_table;
    ```

2. The database is backed up:

    ```bash
    ydb tools dump --path . --output ./my_backup
    ```

3. The database is restored to the `a/b/c` subfolder:

    ```bash
    ydb tools restore --path a/b/c --input ./my_backup
    ```

As the result of the steps described above, the `a/b/c/my_view` view is restored and selects from the `a/b/c/my_table` table:

```bash
ydb sql --script 'select * from `a/b/c/my_view`' --explain
```

The output of the command includes the following line: `TableFullScan (Table: a/b/c/my_table, ...`

### Restoring a subfolder to the database root

Let's consider the following scenario:

1. Steps 1 to 3 of the previous scenario [{#T}](#restoring-database-root-to-a-subfolder) are repeated.
2. The `a/b/c` subfolder of the database is backed up:

    ```bash
    ydb tools dump --path a/b/c --output ./subfolder_backup
    ```

3. The database is cleared:

    ```bash
    ydb scheme rmdir --force --recursive .
    ```

4. The subfolder backup is restored to the root of the database:

    ```bash
    ydb tools restore --path . --input ./subfolder_backup
    ```

As the result of the steps described above, the `my_view` view is restored and selects from the `my_table` table:

```bash
ydb sql --script 'select * from my_view' --explain
```

The output of the command includes the following line: `TableFullScan (Table: my_table, ...`

### Restoring database root to the root of a different database

Let's consider the following scenario:

1. A view is created using the following query:

    ```sql
    CREATE VIEW root_view WITH security_invoker = TRUE AS
        SELECT * FROM root_table;
    ```

2. The database is backed up:

    ```bash
    ydb --endpoint <endpoint> --database /my_database tools dump --path . --output ./my_backup
    ```

    Note the `--database /my_database` option in the connection string.

3. The database backup is restored to a different database:

    ```bash
    ydb --endpoint <endpoint> --database /restored_database tools dump --path . --input ./my_backup
    ```

    Note the `--database /restored_database` option in the connection string.

As the result of the steps described above, `root_view` is restored, and it selects from `root_table` located in `/restored_database`:

```bash
ydb --endpoint <endpoint> --database /restored_database sql --script 'select * from root_view' --explain
```

The output of the command includes the following line: `TableFullScan (Table: root_table, ...`

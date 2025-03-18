# Backup and restoration of views

This article explains how [views](../../../concepts/datamodel/view.md) are backed up and restored in {{ ydb-short-name }}. The [view query rewriting](#view-query-rewrite) described here is relevant for both:

- local backups (see [ydb tools dump](./_includes/tools_dump.md) and [ydb tools restore](./_includes/tools_restore.md) commands)
- and S3 backups (see [ydb export s3](./_includes/s3_export.md) and [ydb import s3](./_includes/s3_import.md) commands)

## View query rewrite

Views are backed up as [CREATE VIEW](../../../yql/reference/syntax/create-view.md) YQL definitions (please refer to the "[File structure of an export](file-structure.md#views)" article for details). During restoration from a backup, these plain-text definitions require special processing to adjust [object reference paths](#object-reference-paths) within view queries to ensure correctness in the target database.

### Object reference paths

Views can reference tables or other views using path-based references, which may be absolute or relative. Absolute paths start with the `'/'` symbol. Relative paths are prepended with the [TablePathPrefix](../../../yql/reference/syntax/pragma#table-path-prefix) pragma value to get an absolute path to the referenced object.

{% note info %}

An important consideration is that the `TablePathPrefix` pragma is never actually empty or unset. If a user doesn't explicitly specify a value for it, the system automatically assigns a default value equal to the database root.

{% endnote %}

For example, a view is created with the following query:

```sql
CREATE VIEW `view` WITH (security_invoker = TRUE) AS
    SELECT
        seasons.title AS seasons_title,
        episodes.title AS episodes_title,
        seasons.series_id AS series_id
    FROM `/my_database_root/seasons`
        AS seasons
    JOIN `episodes`
        AS episodes
    ON seasons.series_id == episodes.series_id
```

In the `SELECT` query of the view we see:

- `/my_database_root` - the root of the database
- `/my_database_root/seasons` - a reference to an object (a table or a view) expressed as an absolute path
- `episodes` - an object reference expressed as a relative path, which corresponds to the absolute path `/my_database_root/episodes`

### Query rewriting rules

Queries are modified according to the following rules:

- The original object reference path is split as follows:

    ```text
    original_reference_path = backup_root + reference_path_relative_to_backup_root
    ```

    where `backup_root` is the value of the `--path` option of the [ydb tools dump](./_includes/tools_dump.md) command

- Upon restoration, each reference path is rewritten to target an object with the same relative location under the restore root:

    ```text
    reference_path_after_restoration = restore_root + reference_path_relative_to_backup_root
    ```

    where `restore_root` is the value of the `--path` option of the [ydb tools restore](./_includes/tools_restore.md) command

This ensures that after restoration, the references within the view correctly point to the intended tables or objects in the new environment.

#### Handling relative paths

In short, [object reference paths](#object-reference-paths) expressed as absolute paths are handled by replacing the `backup_root` prefix with the `restore_root`. Relative reference paths are processed according to the same logic. More precisely, relative paths are first resolved to absolute paths using the [TablePathPrefix](../../../yql/reference/syntax/pragma#table-path-prefix) pragma:

```text
effective_absolute_path = table_path_prefix + relative_path
```

Provided that `table_path_prefix` is a subpath of the backup root, it is sufficient to rewrite just the pragma value, without modifying the relative path. The restoration procedure rewrites it according to the general rules:

1. The original `table_path_prefix` is split into the combination:

    ```text
    original_table_path_prefix = backup_root + table_path_prefix_relative_to_backup_root
    ```

2. The restored `table_path_prefix` is built as follows:

    ```text
    table_path_prefix_after_restoration = restore_root + table_path_prefix_relative_to_backup_root
    ```

If the original view implicitly used the database root as its `TablePathPrefix`, but the restoration occurs at a location different from the target database root, the restoration process automatically injects an explicit pragma `TablePathPrefix` directive into the view restoration query. This can be observed by inspecting the restored view's query definition using the command:

```bash
ydb scheme describe <restored_view_path>
```

### Examples

#### Absolute path

Let's consider the following scenario:

1. A user creates a view with the following query:

    ```sql
    CREATE VIEW my_view WITH security_invoker = TRUE AS
        SELECT * FROM `/my_database/my_table`;
    ```

    Note that the table `my_table` is referenced using its absolute path.

2. The user performs a backup of the database using {{ ydb-short-name }} CLI:

    ```bash
    ydb --database /my_database --endpoint <endpoint> tools dump --path . --output ./my_backup
    ```

3. Later, the user creates a new database, for instance: `target_db`.

4. The user restores the database from the previously created backup:

    ```bash
    ydb --database /target_db --endpoint <endpoint> tools restore --path . --input ./my_backup
    ```

    The {{ ydb-short-name }} CLI automatically rewrites the query stored in the `create_view.sql` file to reference the `/target_db/my_table` table instead of the original `/my_database/my_table`.

#### Implicit table path prefix

Consider the following scenario:

1. The original view was created without an explicit table path prefix:

    ```sql
    CREATE VIEW my_view WITH security_invoker = TRUE AS
        SELECT * FROM my_table;
    ```

2. A backup was generated with the following command:

    ```bash
    ydb --database /my_database --endpoint <endpoint> tools dump --path . --output ./my_backup
    ```

3. The backup was restored into the same database, but into a different subfolder:

    ```bash
    ydb --database /my_database --endpoint <endpoint> tools restore --path ./restore/point --input ./my_backup
    ```

4. As a consequence, the view definition has been modified to reference the `/my_database/restore/point/my_table` instead of the originally referenced `/my_database/my_table`.

5. To confirm the query modification, get the restored view query text with the following command:

    ```bash
    ydb scheme describe restore/point/my_view
    ```

    Command output includes the following `TablePathPrefix` pragma:

    ```text
    <view> my_view

    Query text:
    PRAGMA TablePathPrefix = '/my_database/restore/point';
    SELECT * FROM my_table
    ```

# View backup and restoration

This article explains the backup and restoration behavior of [views](../../../concepts/datamodel/view.md) in {{ ydb-short-name }}.

## Backup file organization

In most aspects, backing up views does not differ from backing up tables:

- Each schema object (table, view, ...) is represented in a backup as a directory (let's call it `object_directory`), whose relative position within the backup output folder (defined by the `--output` parameter provided in the `ydb tools dump` command) matches the object's relative position within the database backup root directory (defined by the `--path` parameter).
- Each `object_directory` contains a file describing the database object.

However, the formats used to backup views and tables differ:

| Object Type | Backup File                      | Format |
|-------------|----------------------------------|--------|
| Table       | scheme.pb                        | Protobuf `CreateTableRequest` message |
| View        | create_view.sql                  | Plain-text `CREATE VIEW` SQL statement |

Thus, during restoration, tables are restored from the protobuf representation (`scheme.pb`), whereas views are restored by executing the `create_view.sql` statements.

## View query rewrite

As we have discussed in the previous [paragraph](#backup-file-organization), {{ ydb-short-name }} views are backed up as `CREATE VIEW` SQL definitions. During restoration from a backup, these plain-text definitions require special processing to adjust object reference paths within view queries to ensure correctness in the target database.

### Example

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

4. When the user restores the database from the previously created backup:

```bash
ydb --database /target_db --endpoint <endpoint> tools restore --path . --input ./my_backup
```

the {{ ydb-short-name }} CLI automatically rewrites the query stored in the `create_view.sql` file to reference the `/target_db/my_table` table instead of the original `/my_database/my_table`.

### Query rewriting rules

The query rewriting follows these rules:

- The original object reference path is split as follows:

```text
original_reference_path = backup_root + reference_path_relative_to_backup_root
```

- Upon restoration, each reference path is rewritten to target an object with the same relative location under the restore root:

```text
reference_path_after_restoration = restore_root + reference_path_relative_to_backup_root
```

This ensures that after restoration, the references within the view correctly point to the intended tables or objects in the new environment.

#### Handling relative paths

Views can reference tables or other views using path-based references, which may be absolute or relative. Absolute reference paths are handled by replacing the `backup_root` path part with the `restore_root`. Relative reference paths follow similar logic. More precisely, relative paths are first resolved to absolute paths using the [TablePathPrefix](../../../yql/reference/syntax/pragma#table-path-prefix) pragma:

```text
effective_absolute_path = TablePathPrefix + relative_path
```

Provided that `TablePathPrefix` is a subpath of the backup root, it is sufficient to rewrite just the pragma value, without altering the relative path. The restoration procedure rewrites it according to the general rules:

1. The original `TablePathPrefix` is split into the combination:

```text
original_table_path_prefix = backup_root + table_path_prefix_relative_to_backup_root
```

2. The restored `TablePathPrefix` is constructed as follows:

```text
table_path_prefix_after_restoration = restore_root + table_path_prefix_relative_to_backup_root
```

An important consideration is that the `TablePathPrefix` pragma is never actually empty or unset. If a user doesn't explicitly specify a value for it, the system automatically assigns a default value equal to the database root. Therefore, if the original view implicitly used the database root as its `TablePathPrefix`, but the restoration occurs at a location different from the target database root, the restoration process automatically injects an explicit pragma `TablePathPrefix` directive into the view restoration query. This can be observed by inspecting the restored view's query definition using the command:

```bash
ydb scheme describe <restored_view_path>
```

#### Example

Consider the following scenario:

1. The original view was created without an explicit table path prefix:

```sql
CREATE VIEW my_view WITH security_invoker = TRUE AS
SELECT * FROM my_table;
```

2. A backup was generated with:

```bash
ydb --database /my_database --endpoint <endpoint> tools dump --path . --output ./my_backup
```

3. The backup was restored into the same database, but into a different subfolder:

```bash
ydb --database /my_database --endpoint <endpoint> tools restore --path ./restore/point --input ./my_backup
```

4. As a consequence, the view definition has been built to reference the `/my_database/restore/point/my_table` instead of the originally referenced `/my_database/my_table`.

5. Confirming this, the restored view query text obtained via:

```bash
ydb scheme describe restore/point/my_view
```

will show the following `TablePathPrefix` pragma:

```text
<view> my_view

Query text:
PRAGMA TablePathPrefix = '/my_database/restore/point';
SELECT * FROM my_table
```

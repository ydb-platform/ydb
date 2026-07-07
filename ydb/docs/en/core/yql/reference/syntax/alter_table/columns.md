# Changing columns

{{ backend_name }} supports adding columns to {% if backend_name == "YDB" and oss == true %} row and column tables{% else %} tables {% endif %}, deleting non-key columns from tables, changing properties of existing columns, and renaming columns of {% if backend_name == "YDB" and oss == true %}row tables{% else %}tables{% endif %}.

## ADD COLUMN

Builds a new column with the specified name, type, and options for the specified table.

```yql
ALTER TABLE table_name ADD COLUMN column_name column_data_type [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])] [ENCODING([OFF|DICT])];
```

## Request parameters

### table_name

The path of the table to which you want to add a new column.

### column_name

The name of the column to be created. When choosing a name for the column, consider the common [column naming rules](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

The data type of the column. The complete list of data types supported by {{ ydb-short-name }} is available in the [{#T}](../../types/index.md) section.

{% include [column_option_list.md](../_includes/column_option_list.md) %}

## Example

The code below will add a column named `views` with data type `Uint64` to the `episodes` table.

```yql
ALTER TABLE episodes ADD COLUMN views Uint64;
```

The code below will add a column named `rate` with data type `Double` and default value `5.0` to the `episodes` table.

```yql
ALTER TABLE episodes ADD COLUMN rate Double NOT NULL DEFAULT 5.0;
ALTER TABLE episodes ADD COLUMN rate Double (DEFAULT 5.0, NOT NULL); -- alternative syntax
```

## ALTER COLUMN

Modifies properties of an existing column in the specified table. Property changes are applied without recreating the column. Some properties apply only to newly written data or during compaction (see the description of each property for details).

```yql
ALTER TABLE table_name ALTER COLUMN column_name SET [FAMILY <family_name>] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])] [ENCODING([OFF|DICT])];
ALTER TABLE table_name ALTER COLUMN column_name DROP [FAMILY] [NOT NULL] [DEFAULT] [COMPRESSION] [ENCODING];
```

### Request parameters

#### table_name

The path of the table containing the column to change.

#### column_name

The name of the column to change in the specified table.

#### SET

Set a column property.

#### DROP

Remove a column property.

{% include [column_option_list_alter.md](../_includes/column_option_list_alter.md) %}

A single `ALTER TABLE` statement can specify multiple `ALTER COLUMN` actions separated by commas.

### Examples

The code below sets a default value for the `rate` column of the `episodes` table.

```yql
ALTER TABLE episodes ALTER COLUMN rate SET DEFAULT 5.0;
```

The code below changes default values for several columns of a table in a single statement — setting new defaults for `col_1` and `col_2` and clearing the default for `col_3`.

```yql
ALTER TABLE default_columns
    ALTER COLUMN col_1 SET DEFAULT "new_a"u,
    ALTER COLUMN col_2 SET DEFAULT 99,
    ALTER COLUMN col_3 DROP DEFAULT;
```

Reset column compression settings:

```yql
ALTER TABLE compressed_table ALTER COLUMN info SET COMPRESSION();
```

After the query runs, the column uses the default compression algorithm again (see the `COMPRESSION` option above).

Enable dictionary encoding on a column:

```yql
ALTER TABLE movies ALTER COLUMN genre SET ENCODING(DICT);
```

## DROP COLUMN

Deletes a column with the specified name from the specified table.

```yql
ALTER TABLE table_name DROP COLUMN column_name;
```

### Request parameters

#### table_name

The path of the table from which you want to delete a column.

#### column_name

The name of the column to be deleted.

### Example

The code below will delete the column named `views` from the `episodes` table.

```yql
ALTER TABLE episodes DROP COLUMN views;
```

## RENAME COLUMN

Renames an existing column, including a column that is part of the primary key. The rename is a pure metadata change: no data is rewritten, since rows are physically addressed by an internal numeric column identifier, not by name.

```yql
ALTER TABLE table_name RENAME COLUMN column_name TO new_column_name;
```

`RENAME COLUMN` cannot be combined with other actions in the same `ALTER TABLE` statement.

### Request parameters

#### table_name

The path of the table containing the column to rename.

#### column_name

The current name of the column.

#### new_column_name

The new name for the column. It must not already be used by another live column of the table, and it is subject to the same [column naming rules](../../../../concepts/datamodel/table.md#column-naming-rules) as a new column.

### Example

The code below renames the `views` column of the `episodes` table to `view_count`.

```yql
ALTER TABLE episodes RENAME COLUMN views TO view_count;
```

{% note warning %}

A column rename is rejected if the column is used as a key or a covered (data) column of a secondary index of a specialized type (vector, full-text, or local bloom/min-max filter indexes) — rename such a column after dropping the affected index, then recreate the index under the new name. Renaming a column referenced by a plain global secondary index is supported: the index definition and its underlying implementation table are updated automatically as part of the same operation.

A column rename is also rejected while the table has an active [changefeed](../../../../concepts/cdc.md) using a JSON-family output format (`JSON`, `DYNAMODB_STREAMS_JSON`, or `DEBEZIUM_JSON`), because those formats embed every column's *current* name as a JSON key in every emitted record — a rename would silently change this contract for existing consumers. Drop and recreate the changefeed after the rename, or explicitly acknowledge the format change if you have already updated the consumer side. This restriction does not apply to changefeeds using the raw protobuf format. If the changefeed feeds an active asynchronous replication target, the rename is rejected unconditionally, with no way to override it — stop or drop the replication target first.

{{ backend_name }} does not track which [views](../create-view.md) depend on a given table's columns. Renaming a column does not automatically update views defined with a query that references it (including via `SELECT *`) — such views may return an error or unexpected results after the rename. Review and, if necessary, recreate any dependent views after renaming a column.

Statistics collected for a column (via `ANALYZE`) are retained across a rename, since they are keyed by the column's internal identifier rather than by its name.

{% endnote %}

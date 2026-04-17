# Changing columns

{{ backend_name }} supports adding columns to {% if backend_name == "YDB" %} [row](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column](../../../../concepts/datamodel/table.md#column-oriented-tables) tables{% else %} [tables](../../../../concepts/datamodel/table.md) {% endif %}, deleting non-key columns from tables, and changing properties of existing columns.

## ADD COLUMN

Builds a new column with the specified name, type, and options for the specified table.

```yql
ALTER TABLE table_name ADD COLUMN column_name column_data_type [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])];
```

## Request parameters

### table_name

The path of the table to be modified.

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
ALTER TABLE table_name ALTER COLUMN column_name {SET | DROP} [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>] [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])];
```

### Request parameters

#### table_name

The path of the table containing the column to change.

#### column_name

The name of the column to change in the specified table.

#### SET

Set a column option.

#### DROP

Remove a column option. Currently only `NOT NULL` can be removed.

{% include [column_option_list.md](../_includes/column_option_list.md) %}

### Examples

The code below will disallow `NULL` values in the `title` column of the `episodes` table.

```yql
ALTER TABLE episodes ALTER COLUMN title SET NOT NULL;
```

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

Reset column compression settings:

```yql
ALTER TABLE compressed_table ALTER COLUMN info SET COMPRESSION();
```

After the query runs, the column uses the default compression algorithm again (see the `COMPRESSION` option above).


## DROP COLUMN

Deletes a column with the specified name from the specified table.

```yql
ALTER TABLE table_name DROP COLUMN column_name;
```

### Request parameters

#### table_name

The path of the table to be modified.

#### column_name

The name of the column to be deleted.

### Example

The code below will delete the column named `views` from the `episodes` table.

```yql
ALTER TABLE episodes DROP COLUMN views;
```

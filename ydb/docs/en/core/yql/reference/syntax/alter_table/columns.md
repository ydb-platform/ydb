# Changing the composition of columns

{{ backend_name }} supports adding columns to {% if backend_name == "YDB" %} [row](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column](../../../../concepts/datamodel/table.md#column-oriented-tables) tables{% else %} [tables](../../../../concepts/datamodel/table.md) {% endif %}, as well as deleting non-key columns from tables.

## ADD COLUMN

Builds a new column with the specified name, type, and options for the specified table.

```yql
ALTER TABLE table_name ADD COLUMN column_name column_data_type [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>];
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
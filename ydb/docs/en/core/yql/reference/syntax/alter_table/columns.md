# Changing the composition of columns

{{ backend_name }} supports adding columns to {% if backend_name == "YDB" %} [row](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column](../../../../concepts/datamodel/table.md#column-oriented-tables) tables{% else %} [tables](../../../../concepts/datamodel/table.md) {% endif %}, as well as deleting non-key columns from tables.

`ADD COLUMN` — adds a column with the specified name and type. The code below will add a column named `views` with data type `Uint64` to the `episodes` table.

```yql
ALTER TABLE episodes ADD COLUMN views Uint64;
```

`DROP COLUMN` — deletes a column with the specified name. The code below will delete the column named `views` from the `episodes` table.

```yql
ALTER TABLE episodes DROP COLUMN views;
```

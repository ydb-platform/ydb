## WeakField {#weakfield}

Fetches a table column from a strong schema, if it is in a strong schema, or from the `_other` and `_rest` fields. If the value is missing, it returns `NULL`.

Syntax: `WeakField([<table>.]<field>, <type>[, <default_value>])`.

The default value is used only if the column is missing in the data schema. To use the default value in any case, use [COALESCE](#coalesce).

**Examples:**

```yql
SELECT
    WeakField(my_column, String, "no value"),
    WeakField(my_table.other_column, Int64)
FROM my_table;
```


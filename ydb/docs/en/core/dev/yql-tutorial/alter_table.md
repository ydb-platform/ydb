# Adding and deleting columns

Add a new column to the table and then delete it.

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

## Adding a column {#add-column}

Add a non-key column to the existing table:

```yql
ALTER TABLE episodes ADD COLUMN viewers Uint64;
```

## Deleting a column {#delete-column}

Delete the column you added from the table:

```yql
ALTER TABLE episodes DROP COLUMN viewers;
```


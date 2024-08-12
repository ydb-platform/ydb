# Selecting data from all columns

Select all columns from the table using [SELECT](../../yql/reference/syntax/select/index.md):

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
SELECT         -- Data selection operator.

    *          -- Select all columns from the table.

FROM episodes; -- The table to select the data from.

COMMIT;
```


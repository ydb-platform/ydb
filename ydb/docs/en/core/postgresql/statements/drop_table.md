# DROP TABLE

{% include [alert_preview.md](../_includes/alert_preview.md) %}

Syntax of the `DROP TABLE` statement:

{% include [syntax](../_includes/statements/drop_table/syntax.md) %}

The `DROP TABLE <table name>;` statement is used to delete a table. For example: `DROP TABLE people;`. If the table being deleted does not exist – an error message will be displayed:
```
Error: Cannot find table '...' because it does not exist or you do not have access permissions. 
Please check correctness of table path and user permissions., code: 2003.
```

In a number of scenarios, such behavior is not required. For example, if we want to ensure the creation of a new table by deleting the previous one within a single SQL script or a sequence of SQL commands. In such cases, the instruction `DROP TABLE IF EXISTS <table name>` is used. If the table does not exist, the instruction will return a `DROP TABLE` message, not an error.

{% include [alert_locks](../_includes/alert_locks.md) %}
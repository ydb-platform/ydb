# DELETE FROM

{% include [alert_preview.md](../_includes/alert_preview.md) %}

Syntax of the `DELETE FROM` statement:

{% include [syntax](../_includes/statements/delete_from/syntax.md) %}

To delete a row from a table based on a specific column value, the construction `DELETE FROM <table name> WHERE <column name><condition><value/range>` is used.


{% note warning %}

Note that the use of the `WHERE ...` clause is optional, so when working with `DELETE FROM` it is very important to avoid accidentally executing the command before specifying the `WHERE ...` clause.

{% endnote %}


{% include [alert_locks](../_includes/alert_locks.md) %}
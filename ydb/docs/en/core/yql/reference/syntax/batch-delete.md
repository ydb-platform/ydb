# BATCH DELETE

{% note tip %}

Before diving into `BATCH DELETE`, it is recommended to familiarize yourself with the standard [DELETE FROM](delete.md).

{% endnote %}

`BATCH DELETE` allows to non-transactionally delete a large number of rows from tables to circumvent transaction limits and risk of transaction lock invalidation failures. It independently applies deletions to each partition of the specified table, removing a limited number of rows per iteration (by default, 10000 rows).

This query, like the standard `DELETE`, is executed synchronously and completes with some status. If an error occurs or the client is disconnected, the data delete stops, and the applied changes are not rolled back.

The semantics are inherited from the standard `DELETE` with the following restrictions:

* Supported only for [row-oriented tables](../../../concepts/glossary.md#row-oriented-table).
* The use of subqueries and multiple queries in a single expression, including `DELETE FROM ... ON`, is prohibited.
* The `RETURNING` keyword is unavailable.

### Example

```yql
BATCH DELETE FROM my_table
WHERE Key1 > 1 AND Key2 >= "One";
```

# BATCH DELETE

{% note tip %}

Before diving into `BATCH DELETE`, it is recommended to familiarize yourself with the standard [DELETE FROM](delete.md).

{% endnote %}

`BATCH DELETE` allows to delete records in large tables while minimizing the risk of lock invalidation and transaction rollback by weakening guarantees. Specifically, data deletion is performed as a series of transactions for each [partition](../../../concepts/datamodel/table.md#partitioning) of the specified table separately, processing a limited number of rows per iteration (by default, 10 000 rows). At the time of request execution, a limited number of partitions are processed simultaneously (by default, no more than 10 partitions).

This query, like the standard `DELETE`, is executed synchronously and completes with some status. If an error occurs or the client is disconnected, the data delete stops, and the applied changes are not rolled back.

The semantics are inherited from the standard `DELETE` with the following restrictions:

* Supported only for [row-oriented tables](../../../concepts/glossary.md#row-oriented-table).
* Supported only for transactions in the `NoTx` mode.
* The use of subqueries and multiple queries in a single expression, including `DELETE FROM ... ON`, is prohibited.
* The `RETURNING` keyword is unavailable.

### Example

```yql
BATCH DELETE FROM my_table
WHERE Key1 > 1 AND Key2 >= "One";
```

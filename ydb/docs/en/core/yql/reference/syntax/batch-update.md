# BATCH UPDATE

{% note tip %}

Before diving into `BATCH UPDATE`, it is recommended to familiarize yourself with the standard [UPDATE](update.md).

{% endnote %}

`BATCH UPDATE` allows to update records in large tables while minimizing the risk of lock invalidation and transaction rollback by weakening guarantees. Specifically, data updates are performed as a series of transactions for each [partition](../../../concepts/datamodel/table.md#partitioning) of the specified table separately, processing a limited number of rows per iteration (by default, 10 000 rows). At the time of request execution, a limited number of partitions are processed simultaneously (by default, no more than 10 partitions).

This query, like the standard `UPDATE`, is executed synchronously and completes with some status. If an error occurs or the client is disconnected, the data update stops, and the applied changes are not rolled back.

The semantics are inherited from the standard `UPDATE` with the following restrictions:

* Supported only for [row-oriented tables](../../../concepts/glossary.md#row-oriented-table).
* Supported only for the implicit transaction control.
* Only idempotent updates are supported: expressions following `SET` should not depend on the current values of the columns being modified.
* The use of subqueries and multiple queries in a single expression, including `UPDATE ON`, is prohibited.
* The `RETURNING` keyword is unavailable.

### Example

```yql
BATCH UPDATE my_table
SET Value1 = YQL::ToString(0), Value2 = 0
WHERE Key1 > 1;
```

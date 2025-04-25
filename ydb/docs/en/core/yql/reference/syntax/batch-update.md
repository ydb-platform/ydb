# BATCH UPDATE

{% note tip %}

Before diving into `BATCH UPDATE`, it is recommended to familiarize yourself with the standard [UPDATE](update.md).

{% endnote %}

`BATCH UPDETE` allows to non-transactionally update a large number of rows from tables to circumvent transaction limits and risk of transaction lock invalidation failures. It independently applies changes to each partition of the specified table, processing a limited number of rows per iteration (by default, 10000 rows).

This query, like the standard `UPDATE`, is executed synchronously and completes with some status. If an error occurs or the client is disconnected, the data update stops, and the applied changes are not rolled back.

The semantics are inherited from the standard `UPDATE` with the following restrictions:

* Supported only for [row-oriented tables](../../../concepts/glossary.md#row-oriented-table).
* Only idempotent updates are supported: expressions following `SET` should not depend on the current values of the columns being modified.
* The use of subqueries and multiple queries in a single expression, including `UPDATE ON`, is prohibited.
* The `RETURNING` keyword is unavailable.

### Example

```yql
BATCH UPDATE my_table
SET Value1 = YQL::ToString(0), Value2 = 0
WHERE Key1 > 1;
```

# BATCH UPDATE

{% note tip %}

Before diving into `BATCH UPDATE`, it is recommended to familiarize yourself with the standard [UPDATE](update.md).

{% endnote %}

`BATCH UPDATE` allows to update records in large tables while minimizing the risk of lock invalidation and transaction rollback by weakening guarantees. Specifically, data updates are performed as a series of transactions for each [partition](../../../concepts/datamodel/table.md#partitioning) of the specified table separately, processing 10 000 rows per iteration. Each query processes up to 10 partitions concurrently.

This query, like the standard `UPDATE`, executes synchronously and returns a status. If an error occurs or the client disconnects, the data update stops, and the applied changes are not rolled back.

The semantics are inherited from the standard `UPDATE` with the following restrictions:

* Supported only for [row-oriented tables](../../../concepts/glossary.md#row-oriented-table).
* Supported only for queries with implicit transaction control (`NoTx` mode or `EmptyTxControl` in SDK).
* Only idempotent updates are supported: expressions following `SET` should not depend on the current values of the columns being modified.
* The use of subqueries and multiple statements in a single query is prohibited.
* The `RETURNING` clause is unavailable.

## Example

```yql
BATCH UPDATE my_table
SET Value1 = "foo", Value2 = 0
WHERE Key1 > 1;
```

{% if tech %}

## YDB

### `ydb.CostBasedOptimizationLevel` {#costbasedoptimizationlevel}

| Level | Description |
| ----- | ------------------------ |
| 0     | Cost-based optimizer is disabled. |
| 1     | Cost-based optimizer is disabled, but estimates are computed and available. |
| 2     | Cost-based optimizer is enabled only for queries that include [column-oriented tables](../../../../../concepts/glossary.md#column-oriented-table). |
| 3     | Cost-based optimizer is enabled for all queries, but `LookupJoin` is preferred for row-oriented tables. |
| 4     | Cost-based optimizer is enabled for all queries. |

### `kikimr.IsolationLevel`

| Value type | Default |
| --- | --- |
| Serializable, ReadCommitted, ReadUncommitted, or ReadStale. | Serializable |

An experimental pragma that allows you to reduce the isolation level of the current YDB transaction.

{% endif %}


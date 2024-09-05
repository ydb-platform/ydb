{% if tech %}

## YDB

### `ydb.CostBasedOptimizationLevel` {#costbasedoptimizationlevel}

| Level | Description |
| ------- | ---------------------- |
| 0 | Cost Based Optimizer is turned off |
| 1 | Cost Based Optimizer is turned off, but estimates are computed and avaliable |
| 2 | Cost Based Optimizer is turned on only for queries that include [column oriented tables](../../../../../concepts/glossary.md#column-oriented-table) |
| 3 | Cost Based Optimizer is turned on for all queries, but LookupJoin is prefered for row oriented tables |
| 4 | Cost Based Optimizer is turned on for all queries |

### `kikimr.IsolationLevel`

| Value type | Default |
| --- | --- |
| Serializable, ReadCommitted, ReadUncommitted, or ReadStale. | Serializable |

An experimental pragma that allows you to reduce the isolation level of the current YDB transaction.

{% endif %}


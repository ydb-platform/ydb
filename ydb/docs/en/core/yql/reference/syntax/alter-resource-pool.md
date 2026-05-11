# ALTER RESOURCE POOL

`ALTER RESOURCE POOL` changes the definition of a [resource pool](../../../concepts/glossary.md#resource-pool).

## Syntax

### Changing parameters

The syntax for changing any resource pool parameter is as follows:

```yql
ALTER RESOURCE POOL <name> SET (<key> = <value>);
```

`<key>` is the parameter name, `<value>` is its new value.

For example, the following command sets a limit of 100 concurrent queries:

```yql
ALTER RESOURCE POOL olap SET (CONCURRENT_QUERY_LIMIT = "100");
```

### Resetting parameters

The command to reset a resource pool parameter is as follows:

```yql
ALTER RESOURCE POOL <name> RESET (<key>);
```

`<key>` is the parameter name.

For example, the following command resets `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` for the resource pool:

```yql
ALTER RESOURCE POOL olap RESET (TOTAL_CPU_LIMIT_PERCENT_PER_NODE);
```

## Permissions

The `ALTER SCHEMA` [permission](grant.md#permissions-list) on the resource pool under `.metadata/workload_manager/pools` is required. Example:

```yql
GRANT 'ALTER SCHEMA' ON `.metadata/workload_manager/pools/olap_pool` TO `user1@domain`;
```

## Parameters

* `CONCURRENT_QUERY_LIMIT` (Int32) — Optional: maximum number of queries executing in parallel in the resource pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `QUEUE_SIZE` (Int32) — Optional: wait queue size. The system may hold at most $CONCURRENT\_QUERY\_LIMIT + QUEUE\_SIZE$ queries at once. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `DATABASE_LOAD_CPU_THRESHOLD` (Int32) — Optional: database-wide CPU load threshold above which queries are not started and remain queued. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available memory on a node that a single query in this pool may use. If `-1`, the limit is shared total available memory across all queries. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available CPU on a node that all queries in this pool may use together. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_CPU_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available CPU on a node for a single query in the pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `RESOURCE_WEIGHT` (Int32) — Optional: weight for distributing resources among pools. If `-1`, weights are not used. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool.md)
* [{#T}](drop-resource-pool.md)

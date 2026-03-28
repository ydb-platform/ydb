# CREATE RESOURCE POOL

`CREATE RESOURCE POOL` creates a [resource pool](../../../concepts/glossary.md#resource-pool).

## Syntax

```yql
CREATE RESOURCE POOL <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

* `name` — name of the resource pool to create. Must be unique. Must not be written as a path (no `/`).
* `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — optional list of parameters that control pool behavior.

### Parameters {#parameters}

* `CONCURRENT_QUERY_LIMIT` (Int32) — Optional: maximum number of queries executing in parallel in the pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `QUEUE_SIZE` (Int32) — Optional: wait queue size. At most $CONCURRENT\_QUERY\_LIMIT + QUEUE\_SIZE$ queries may be present at once. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `DATABASE_LOAD_CPU_THRESHOLD` (Int32) — Optional: database-wide CPU load threshold above which queries are not started and remain queued. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available memory on a node that a single query in this pool may use. If `-1`, the limit is shared total available memory across all queries. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available CPU on a node that all queries in this pool may use together. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_CPU_LIMIT_PERCENT_PER_NODE` (Double) — Optional: percentage of available CPU on a node for a single query in the pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `RESOURCE_WEIGHT` (Int32) — Optional: weight for distributing resources among pools. If `-1`, weights are not used. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.

## Notes {#remark}

Queries always run in some resource pool. By default they go to the `default` pool, which is created automatically, cannot be removed, and is always present.

If `CONCURRENT_QUERY_LIMIT` is set to `0`, all queries sent to this pool fail immediately with status `PRECONDITION_FAILED`.

## Permissions

The `CREATE TABLE` [permission](grant.md#permissions-list) on the `.metadata/workload_manager/pools` directory is required. Example:

```yql
GRANT 'CREATE TABLE' ON `.metadata/workload_manager/pools` TO `user1@domain`;
```

## Examples {#examples}

```yql
CREATE RESOURCE POOL olap WITH (
    CONCURRENT_QUERY_LIMIT=20,
    QUEUE_SIZE=1000,
    DATABASE_LOAD_CPU_THRESHOLD=80,
    RESOURCE_WEIGHT=100,
    QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=80,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
)
```

The example above creates a resource pool with:

- At most 20 concurrent queries.
- Wait queue size 1000.
- When database CPU load reaches 80%, new queries stop starting in parallel.
- Each query may use at most 80% of available memory on a node; exceeding that fails the query with `OVERLOADED`.
- Total CPU for all queries in the pool on a node is capped at 70%.
- Resource weight 100, used when resources are oversubscribed.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool.md)
* [{#T}](drop-resource-pool.md)

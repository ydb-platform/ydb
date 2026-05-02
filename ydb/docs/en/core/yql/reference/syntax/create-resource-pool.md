# CREATE RESOURCE POOL

`CREATE RESOURCE POOL` creates a [resource pool](../../../dev/resource-consumption-management.md).

## Syntax

```yql
CREATE RESOURCE POOL <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```

* `name` — the name of the resource pool being created. It must be unique. Path notation is not allowed (the name must not contain `/`).
* `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` — sets parameters that define the behavior of the resource pool.

### Parameters {#parameters}

* `CONCURRENT_QUERY_LIMIT` (Int32) — optional; the number of queries that can run in parallel in the resource pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `QUEUE_SIZE` (Int32) — optional; the size of the wait queue. The system may hold at most $CONCURRENT\_QUERY\_LIMIT + QUEUE\_SIZE$ queries at once. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.
* `DATABASE_LOAD_CPU_THRESHOLD` (Int32) — optional; the database-wide CPU load threshold after which queries are not dispatched for execution and remain in the queue. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` (Double) — optional; the percentage of available memory on a node that a query in this resource pool may use. If `-1`, the limit is the total available memory shared across all queries. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` (Double) — optional; the percentage of available CPU that all queries in this resource pool may use on a node. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `QUERY_CPU_LIMIT_PERCENT_PER_NODE` (Double) — optional; the percentage of available CPU on a node for a single query in the resource pool. If `-1`, there is no limit. Default: `-1`. Allowed values: $-1, [0, 100]$.
* `RESOURCE_WEIGHT` (Int32) — optional; weights used to distribute resources between pools. If `-1`, weights are not used. Default: `-1`. Allowed values: $-1, [0, 2^{31}-1]$.

## Remarks {#remark}

Queries always run in some resource pool. By default, all queries go to the `default` resource pool, which is created automatically and cannot be removed — it is always present.

If `CONCURRENT_QUERY_LIMIT` is set to `0`, all queries sent to this pool terminate immediately with status `PRECONDITION_FAILED`.

## Permissions

You need the [`CREATE TABLE`](./grant.md#permissions-list) permission on the `.metadata/workload_manager/pools` directory. Example:

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

The example above creates a resource pool with the following limits:

- At most 20 concurrent queries.
- Wait queue size at most 1000.
- When database load reaches 80%, queries stop being launched in parallel.
- Each query in the pool may use at most 80% of available memory on a node. If a query exceeds this limit, it ends with status `OVERLOADED`.
- Total CPU for all queries in the pool on a node is capped at 70%.
- The pool has weight 100, which applies only in case of oversubscription.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)

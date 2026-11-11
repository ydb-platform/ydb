# CREATE RESOURCE POOL

`CREATE RESOURCE POOL` creates a [resource pool](../../../concepts/glossary.md#resource-pool.md).

## Syntax


```yql
CREATE RESOURCE POOL <name>
WITH ( <parameter_name> [= <parameter_value>] [, ... ] )
```


* `name` is the name of the resource pool being created. It must be unique. Path notation is not allowed (i.e., it must not contain `/`).
* `WITH ( <parameter_name> [= <parameter_value>] [, ... ] )` allows setting parameter values that define the behavior of the resource pool.

### Parameters {#parameters}

* `CONCURRENT_QUERY_LIMIT` (Int32) — an optional field that sets the number of concurrently executing queries in the resource pool. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 2^{31}-1]$.
* `QUEUE_SIZE` (Int32) — an optional field that defines the size of the waiting queue. The system can hold no more than $CONCURRENT_QUERY_LIMIT + QUEUE_SIZE$ queries at a time. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 2^{31}-1]$.
* `DATABASE_LOAD_CPU_THRESHOLD` (Int32) — an optional field that sets the CPU load threshold for the entire database, after which queries are not sent for execution and remain in the queue. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 100]$.
* `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` (Double) — an optional field that sets the percentage of available CPU that all queries on the node in this resource pool can use. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 100]$.

## Notes {#remark}

Queries are always executed in some resource pool. By default, all queries are sent to the resource pool `default`, which is created automatically and cannot be deleted — it is always present in the system.

If the value of parameter `CONCURRENT_QUERY_LIMIT` is set to 0, then all queries sent to this pool will be immediately terminated with status `PRECONDITION_FAILED`.

## Permissions

Requires [permission](./grant.md#permissions-list) `CREATE TABLE` on the directory `.metadata/workload_manager/pools`, an example of granting such permission:


```yql
GRANT 'CREATE TABLE' ON `.metadata/workload_manager/pools` TO `user1@domain`;
```


```yql
CREATE RESOURCE POOL olap WITH (
    CONCURRENT_QUERY_LIMIT=20,
    QUEUE_SIZE=1000,
    DATABASE_LOAD_CPU_THRESHOLD=80,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
)
```


In the example above, a resource pool is created with the following limits:

- Maximum number of concurrent queries — 20.
- Maximum queue size — 1000.
- When the database load reaches 80%, queries stop running concurrently.
- The total limit on available CPU for all queries in the pool on the node is 70%.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](alter-resource-pool.md)
* [{#T}](drop-resource-pool.md)

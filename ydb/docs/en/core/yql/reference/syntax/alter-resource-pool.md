# ALTER RESOURCE POOL

`ALTER RESOURCE POOL` changes the definition of a [resource pool](../../../concepts/glossary.md#resource-pool.md).

## Syntax

### Modifying parameters

The syntax for modifying any resource pool parameter is as follows:


```yql
ALTER RESOURCE POOL <name> SET (<key> = <value>);
```


`<key>` is the parameter name, `<value>` is its new value.

For example, the following command enables a limit of 100 concurrent queries:


```yql
ALTER RESOURCE POOL olap SET (CONCURRENT_QUERY_LIMIT = "100");
```


### Resetting parameters

The command to reset a resource pool parameter is as follows:


```yql
ALTER RESOURCE POOL <name> RESET (<key>);
```


```<key>``` is the parameter name.

For example, the following command resets the `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` settings for a resource pool:


```yql
ALTER RESOURCE POOL olap RESET (TOTAL_CPU_LIMIT_PERCENT_PER_NODE);
```


## Permissions

A [permission](grant.md#permissions-list) `ALTER SCHEMA` on the resource pool in the `.metadata/workload_manager/pools` directory is required. Example of granting such a permission:


```yql
GRANT 'ALTER SCHEMA' ON `.metadata/workload_manager/pools/olap_pool` TO `user1@domain`;
```


## Parameters

* `CONCURRENT_QUERY_LIMIT` (Int32) — an optional field that sets the number of concurrently executing queries in the resource pool. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 2^{31}-1]$.
* `QUEUE_SIZE` (Int32) — an optional field that defines the size of the waiting queue. The system can hold no more than $CONCURRENT_QUERY_LIMIT + QUEUE_SIZE$ queries at a time. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 2^{31}-1]$.
* `DATABASE_LOAD_CPU_THRESHOLD` (Int32) — an optional field that sets the CPU load threshold for the entire database, after which queries are not sent for execution and remain in the queue. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 100]$.
* `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` (Double) — an optional field that sets the percentage of available CPU that all queries on the node in this resource pool can use. If the value is `-1`, there is no limit. Default value: `-1`. Valid values: $-1, [0, 100]$.

## See also

* [{#T}](../../../dev/resource-consumption-management.md)
* [{#T}](create-resource-pool.md)
* [{#T}](drop-resource-pool.md)

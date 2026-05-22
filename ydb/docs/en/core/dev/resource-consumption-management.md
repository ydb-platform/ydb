# Workload Manager — Resource Consumption Management

[Resource pools](../concepts/glossary.md#resource-pool) allow you to isolate database resources between running queries or configure resource allocation strategies in case of oversubscription (requesting more resources than are available in the system). All resource pools are equal, without any hierarchy, and affect each other only in case of a general resource shortage.

For example, one of the typical resource isolation scenarios is to separate two classes of consumers:

1. A regular robotic process that generates a report once a day.
2. Analysts who run ad hoc queries.
{% note warning %}
The presented functionality for managing resource consumption is in the Preview stage.
{% endnote %}
## Creating a resource pool

The example below shows the syntax for creating a separate resource pool named "olap" that will be used to run analytical queries:
```yql
CREATE RESOURCE POOL olap WITH (
    CONCURRENT_QUERY_LIMIT=10,
    QUEUE_SIZE=1000,
    DATABASE_LOAD_CPU_THRESHOLD=80,
    RESOURCES_WEIGHT=100,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
)
```
You can find a complete list of resource pool parameters in the reference [{#T}](../yql/reference/syntax/create-resource-pool.md#parameters). Some parameters are global for the entire database (for example, `CONCURRENT_QUERY_LIMIT`, `QUEUE_SIZE`, `DATABASE_LOAD_CPU_THRESHOLD`), while others apply only to a single compute node (for example, `QUERY_CPU_LIMIT_PERCENT_PER_NODE`, `TOTAL_CPU_LIMIT_PERCENT_PER_NODE`, `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE`). CPU can be shared among all pools in case of oversubscription on a single compute node using `RESOURCES_WEIGHT`.

![resource_pools](../_assets/resource_pool.png)

Let's consider the example above to understand what these parameters actually mean and how they affect resource allocation. Suppose {{ ydb-short-name }} database has $10$ nodes with $10 vCPU$ each. In total, such a database has $100 vCPU$. Then, for a resource pool named `olap`, each node will allocate:

$\frac{10 vCPU \cdot TOTAL\_CPU\_LIMIT\_PERCENT\_PER\_NODE}{100} = 10 vCPU \cdot 0.7 = 7 vCPU$

In total, with uniform resource distribution across the entire database, the resource pool will be allocated:

$7 vCPU \cdot 10 \text{ (nodes)} = 70 vCPU$

For a single query in this resource pool, the allocation will be:

$\frac{10 vCPU \cdot TOTAL\_CPU\_LIMIT\_PERCENT\_PER\_NODE}{100} \cdot \frac{QUERY\_CPU\_LIMIT\_PERCENT\_PER\_NODE}{100} = 10 vCPU \cdot 0.7 \cdot 0.5 = 3.5 vCPU$

### How CONCURRENT_QUERY_LIMIT and QUEUE_SIZE work {#concurrent_query_limit}

Suppose there are already 9 queries running in the `olap` resource pool. When a new query arrives, it will immediately start running in parallel with the other 9 queries. Now there will be 10 queries running in the pool. If an 11th query arrives in the pool, it will not start running but will be placed in the waiting queue. When at least one of the 10 running queries completes, the 11th query will be taken out of the queue and start running.

If there are already $QUEUE\_SIZE = 1000$ queries in the queue, sending the 1001st query will result in an immediate error response to the client, and that query will not be executed. Example of an error:
```text
Issues:
<main>: Error: Request was rejected, number of local pending requests is 20, number of global delayed/running requests is 0, sum of them is larger than allowed limit 1 (including concurrent query limit 1) for pool olap
<main>: Error: Query failed during adding/waiting in workload pool olap
```
The number of concurrently executed queries is affected not only by `CONCURRENT_QUERY_LIMIT`, but also by `DATABASE_LOAD_CPU_THRESHOLD`.

### How DATABASE_LOAD_CPU_THRESHOLD works {#database_load_cpu_threshold}

When a query arrives at a resource pool with `DATABASE_LOAD_CPU_THRESHOLD` set, 10% of the available CPU on the node is immediately reserved, based on the assumption that the query will require at least this amount of resources. Then, every 10 seconds, the consumption of resources across the entire database is recalculated, which allows refining the initial 10% estimate. This means that if more than 10 queries arrive at the cluster node simultaneously, no more than 10 queries will be started for execution, and the rest will wait for the actual CPU consumption to be clarified.

As with `CONCURRENT_QUERY_LIMIT`, when the specified load threshold is exceeded, queries are sent to the waiting queue.

### Resource distribution according to RESOURCES_WEIGHT {#resources_weight}

![resource_pools](../_assets/resources_weight.png)

The `RESOURCES_WEIGHT` parameter starts working only in case of oversubscription and when there is more than one resource pool in the system. In the current implementation, `RESOURCES_WEIGHT` affects only the distribution of `vCPU` resources. When queries appear in a resource pool, it begins to participate in resource distribution. For this, the pools recalculate limits according to the [Max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness) algorithm. The resource redistribution itself is performed individually on each compute node, as shown in the figure above.

Let's assume we have a node in the system with $10 vCPU$ available. The following restrictions are set:

- $TOTAL\_CPU\_LIMIT\_PERCENT\_PER\_NODE = 30$,
- $QUERY\_CPU\_LIMIT\_PERCENT\_PER\_NODE = 50$.

In this case, the resource pool will have a limit of $3 vCPU$ per node and $1.5 vCPU$ per query in this pool (figure *a*). If there are 4 such pools in the system and they all try to use maximum resources, this will amount to $12 vCPU$, which exceeds the limit of available resources on the node ($10 vCPU$). In this case, `RESOURCES_WEIGHT` comes into play, and each pool will be allocated $2.5 vCPU$ (figure *b*).

If you need to increase the allocated resources for a specific pool, you can change its weight, for example, to 200. Then this pool will receive $3 vCPU$, and the remaining pools will share the remaining $7 vCPU$ equally, which will amount to $\frac{7}{3} vCPU$ per pool (figure *c*).
{% note warning %}
The current resource allocation algorithm may be changed in the future without backward compatibility support.
{% endnote %}
## Default resource pool

Even if no resource pools have been created, the system always has a `default` resource pool, which cannot be deleted. Any request running in the system always belongs to some pool — there are no situations where a request is not associated with any resource pool. By default, the settings of the `default` resource pool look like this:
```yql
CREATE RESOURCE POOL default WITH (
    CONCURRENT_QUERY_LIMIT=-1,
    QUEUE_SIZE=-1,
    DATABASE_LOAD_CPU_THRESHOLD=-1,
    RESOURCES_WEIGHT=-1,
    QUERY_MEMORY_LIMIT_PERCENT_PER_NODE=-1,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE=-1,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=-1
)
```
This means that no restrictions are applied in the `default` resource pool: it operates independently of other pools and has no limits on the resources it consumes. In the `default` resource pool, you can change parameters using the [{#T}](../yql/reference/syntax/alter-resource-pool.md) request, with the exception of the `CONCURRENT_QUERY_LIMIT`, `DATABASE_LOAD_CPU_THRESHOLD`, and `QUEUE_SIZE` parameters. This restriction is intentional to minimize the risks associated with incorrect configuration of the default resource pool.
## Resource Pool ACL Management

### Permissions for Creating, Modifying, and Deleting a Resource Pool

To create, modify, or delete a resource pool, you need to grant access rights in accordance with the permissions described in the reference [{#T}](../yql/reference/syntax/create-resource-pool.md). For example, to create resource pools, you need to have `CREATE TABLE` permission on the `.metadata/workload_manager/pools` directory, which can be granted with a query of the following type:
```yql
GRANT CREATE TABLE ON `.metadata/workload_manager/pools` TO user1;
```
### Permissions to Execute a Query in a Resource Pool {#run-access}

To execute a query in a pool, a user must have [access permission](../yql/reference/syntax/grant.md#permissions-list) `SELECT` on this pool. Example of granting permissions:
```yql
GRANT SELECT
    ON `.metadata/workload_manager/pools/olap`
    TO `user1@domain`;
```
## Creating a Resource Pool Classifier

[Resource pool classifiers](../concepts/glossary.md#resource-pool-classifier) allow you to set rules for distributing requests among resource pools. The example below shows a resource pool classifier that sends requests from all users to the resource pool named `olap`:
```yql
CREATE RESOURCE POOL CLASSIFIER olap_classifier
WITH (
    RESOURCE_POOL = 'olap',
    MEMBER_NAME = 'all-users@well-known'
);
```
- `RESOURCE_POOL` — the name of the resource pool to which a request that meets the requirements specified in the resource pool classifier will be sent.
- `MEMBER_NAME` — a user group or user whose requests will be sent to the specified resource pool.
## Managing ACL of the Resource Pool Classifier

Resource pool classifiers are global for the entire database and apply to all users. To create, delete, or modify a resource pool classifier, you need to have [access rights](../yql/reference/syntax/grant.md#permissions-list) `USE` for the entire database, which can be granted with a query of the following form:
```yql
GRANT USE ON `/my_db` TO user1;
```

{% note warning %}
To use the classifier, the user must have [access to the resource pool](#run-access) that this classifier refers to. If there is no such access, the classifier is skipped and the next one is checked. In future versions, this behavior may change, so do not use the lack of access rights to the pool as a mechanism for controlling which classifier is triggered.
{% endnote %}
## The order of selecting a resource pool classifier in case of conflicts
```yql
CREATE RESOURCE POOL CLASSIFIER olap1_classifier
WITH (
    RESOURCE_POOL = 'olap1',
    MEMBER_NAME = 'user1@domain'
);

CREATE RESOURCE POOL CLASSIFIER olap2_classifier
WITH (
    RESOURCE_POOL = 'olap2',
    MEMBER_NAME = 'user1@domain'
);
```
Suppose there are two resource pool classifiers with conflicting conditions, and the user `user1@domain` meets the criteria for both resource pools: `olap1` and `olap2`. If no classifiers existed in the system before, then `RANK=1000` is set for `olap1`, and `RANK=2000` for `olap2`. Resource pool classifiers with a lower `RANK` value have a higher priority. In this example, since `olap1` has a higher-priority `RANK` than `olap2`, `olap1` will be selected.

You can also set `RANK` for resource pool classifiers yourself during creation using the syntax construct [{#T}](../yql/reference/syntax/create-resource-pool-classifier.md), or change `RANK` for existing resource pool classifiers using [{#T}](../yql/reference/syntax/alter-resource-pool-classifier.md).

The system cannot have two classifiers with the same `RANK` value, which allows unambiguously determining which resource pool will be selected in case of conflicting conditions.
## Example of a Priority Resource Pool

Let's consider an example of resource allocation between an analytics team and a hypothetical CEO. It is important for the CEO to have priority over the computing resources used for analytical tasks, but it is also useful to provide the analytics team with the opportunity to utilize more cluster resources during periods when the CEO is not using the resources. The configuration for this scenario might look like this:
```yql
CREATE RESOURCE POOL olap WITH (
    CONCURRENT_QUERY_LIMIT=20,
    QUEUE_SIZE=100,
    DATABASE_LOAD_CPU_THRESHOLD=80,
    RESOURCES_WEIGHT=20,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE=80,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=100
);

CREATE RESOURCE POOL the_ceo WITH (
    CONCURRENT_QUERY_LIMIT=20,
    QUEUE_SIZE=100,
    RESOURCES_WEIGHT=100,
    QUERY_CPU_LIMIT_PERCENT_PER_NODE=100,
    TOTAL_CPU_LIMIT_PERCENT_PER_NODE=100
);
```
In the example above, two resource pools are created: `olap` for the analytics team and `the_ceo` for the CEO.

- **Resource pool `olap`**:

  - Has a weight of 20.
  - The limit on running queries when the database is overloaded is 80% of the available resources.

- **Resource pool `the_ceo`**:

  - Has a greater weight — 80.
  - There are no limits on running queries when overloaded.

A weight of 80 for `the_ceo` effectively means that in the competition for resources, the `the_ceo` pool will receive 4 times more priority than the `olap` pool. If queries are received in both pools, the system will recalculate the limits, and for `olap`, the `TOTAL_CPU_LIMIT_PERCENT_PER_NODE` limit will be reduced to 20%, and for `the_ceo` — increased to 80%. This redistribution of resources is based on weights, as described [above](#resources_weight).
## Explicit Selection of a Resource Pool for a Query

If necessary, a user can explicitly specify in which resource pool a given query should be executed. Currently, this can be done in the following ways:

- **Embedded UI** — in the query launch settings window `Query execution settings` via the `Resource pool` parameter.
- **YDB CLI** — in the [`ydb sql`](../reference/ydb-cli/sql.md) command with the `--resource-pool` parameter, for example, `ydb sql --resource-pool my_pool -s "SELECT 1"`.
- **YDB CLI ([interactive mode](../reference/ydb-cli/interactive-cli.md))** — [by the command](../reference/ydb-cli/interactive-cli.md#internal-vars) `SET resource_pool = my_pool`, where `my_pool` is the name of the resource pool.
- **YDB CPP SDK** — in the query launch settings via the [ResourcePool](https://github.com/ydb-platform/ydb/blob/fb05a8472be6b2770528b3e90093e67a7bca8f0e/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h#L111) parameter.
- **YDB GO SDK** — in the `ExecuteOption` query launch settings via the [WithResourcePool](https://pkg.go.dev/github.com/ydb-platform/ydb-go-sdk/v3@v3.133.1/query#WithResourcePool) call.
{% note warning %}
The current version of **YDB Python SDK** does not allow defining the resource pool in which the query should be executed.
{% endnote %}
## Diagnostics

### Query Plan

Detailed information about query plans can be found on the page [Query Plan Structure](../yql/query_plans.md). To get information about the resource pool being used, you need to run a command to obtain statistics in `json-unicode` format. Example command:
```bash
ydb -p <profile_name> sql -s 'select 1' --stats full --format json-unicode
```
In the request plan body obtained using the command provided above, you can find useful attributes for diagnosing work with the resource pool. An example of such information:
```json
"Node Type" : "Query",
"Stats" : {
  "TotalDurationUs": 28795,
  "ProcessCpuTimeUs": 45,
  "Compilation": {
    "FromCache": false,
    "CpuTimeUs": 7280,
    "DurationUs": 21700
  },
  "ResourcePoolId": "default",
  "QueuedTimeUs": 0
},
"PlanNodeType" : "Query"
```
Useful attributes:

- `TotalDurationUs` — total request execution time, including queue waiting time.
- `ResourcePoolId` — the name of the resource pool to which the request was assigned.
- `QueuedTimeUs` — total time the request spent in the queue.

### Status of an executing request

Information about how a request is processed in Workload Manager can be obtained from the system view [`.sys/query_sessions`](system-views.md#query-sessions). This view has the following fields:

- `WmPoolId` `(Utf8)` — the ID of the pool in which the request is being executed.
- `WmState` `(Utf8)` — the status of the request in WM.
- `WmEnterTime` `(Timestamp)` — the time when the request entered the PENDING or DELAYED status.
- `WmExitTime` `(Timestamp)` — the time when the request was sent for execution.

Possible values of the WmState field:

- `NONE` — Not being processed.
- `PENDING` — Being processed (in the process of classification/routing).
- `DELAYED` — In the queue.
- `EXITED` — Sent for execution.

The following query outputs information about all active requests in the system:
```yql
select
    Query,
    WmPoolId,       -- Pool identifier
    WmState,        -- Request status in WM
    WmEnterTime,    -- The time when the request entered the PENDING or DELAYED status
    WmExitTime      -- The time when the request is submitted for execution
from `.sys/query_sessions`
where State = 'EXECUTING'
```
### Metrics

Information about resource pool metrics can be found in the [metrics reference](../reference/observability/metrics/index.md#resource_pools).

### System Views

Information about system views related to resource pools and resource pool classifiers can be found on the page [{#T}](system-views.md#resource_pools).
## See also

- [{#T}](../yql/reference/syntax/create-resource-pool.md)
- [{#T}](../yql/reference/syntax/alter-resource-pool.md)
- [{#T}](../yql/reference/syntax/drop-resource-pool.md)
- [{#T}](../yql/reference/syntax/create-resource-pool-classifier.md)
- [{#T}](../yql/reference/syntax/alter-resource-pool-classifier.md)
- [{#T}](../yql/reference/syntax/drop-resource-pool-classifier.md)

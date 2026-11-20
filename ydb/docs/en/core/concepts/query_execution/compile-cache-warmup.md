# Compilation cache warmup

The first queries after a [database node](../glossary.md#database-node) restarts can take longer to run. Before executing a query, the node determines which operations to perform and in what order. This process is called compilation and produces a query execution plan. The node caches plans in memory for reuse. Restarting the node clears its cache.

Compilation cache warmup reduces this initial delay. {{ ydb-short-name }} prepares plans for [frequently used queries](#query-selection) before the node starts serving clients. This is useful when restarting nodes one at a time or adding a node to a running database.

Warmup runs automatically by default. Administrators can [limit how long it runs and how many queries it prepares](../../reference/configuration/table_service_config.md#compile-cache-warmup). The node only compiles the queries during warmup. They do not run or modify any data.

## How warmup works

1. The starting node reads query information from the caches of other nodes in the same database through the [`.sys/compile_cache_queries`](../../dev/system-views.md#compile-cache-queries) system view.
2. It selects the most frequently used queries from the results. The [warmup settings](../../reference/configuration/table_service_config.md#compile-cache-warmup) limit how many nodes it contacts, how many queries it selects, and how many queries it compiles concurrently.
3. The node compiles each selected query using its user identity, query type, syntax, and parameter types. It stores the resulting plan in its local cache.
4. Once warmup finishes, the node appears in the list of available database [endpoints](../connect.md#endpoint) and starts serving client requests. Until then, a request sent directly to the node may return `UNAVAILABLE` with a message about cache warmup.

To see which queries the node currently has in its cache, filter `.sys/compile_cache_queries` by its `NodeId`. The results also include queries compiled while serving clients. See the [example queries for this view](../../dev/system-views.md#compile-cache-queries-examples).

### Query selection {#query-selection}

The node selects queries based on how often they were accessed on running nodes and how long they took to compile. Depending on the warmup limits, only a subset of the cached queries may be selected.

Warmup supports Table Service and Query Service queries, including parameterized queries.

## Restarts and errors

During a rolling restart, the nodes still running provide cached queries to the node that is starting. A single-node database skips warmup because it has no other node to read from.

The cache is held in memory. If all nodes restart at once, warmup may find no queries to prepare. The cache then fills as client queries arrive.

At the soft deadline, the node stops scheduling new compilations and waits for those already in progress. Warmup ends at the hard deadline. The node can still start serving clients if it fails to retrieve cached queries or compile some of them. Plans that were prepared remain available in the cache.

A client query may need compilation if warmup skipped it, ran out of time, or prepared it for a different user, query type, syntax, or set of parameter types. A plan can also be evicted from the cache or require recompilation after a schema change.

Warmup does not run on system nodes in the root domain. Serverless databases do not support reading the cache through `.sys/compile_cache_queries`.

## Configuration

For settings, default values, and an example, see [Compilation cache warmup](../../reference/configuration/table_service_config.md#compile-cache-warmup) in the configuration reference.

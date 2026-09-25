# Parameterized queries and recompilation

{{ ydb-short-name }} caches compilation results on a cluster [node](../../concepts/glossary.md#node): the [compile cache](../../concepts/glossary.md#compile-cache) reuses them only when the query **text matches exactly**. If your application builds YQL with string concatenation or formatting, each new set of values produces a **different text**, and the server recompiles it even when the SQL structure is the same.

Consequences:

- higher latency at the compilation stage;
- increased CPU load on nodes;
- with many unique query texts, reuse of the limited per-node cache gets worse.

A **parameterized query** helps avoid extra compilation: the YQL text stays fixed, and input values are passed separately via [named parameters](../../yql/reference/syntax/declare.md) (for example, `$userId`). Below, both approaches are compared on the same example.

## Embedding values in the query text

The application inserts values directly into the query text:

```yql
SELECT id, name FROM users WHERE id = 123 AND status = "active";
SELECT id, name FROM users WHERE id = 456 AND status = "inactive";
```

The queries differ only in values, but the texts are different — for the server these are two different queries, and each one is compiled separately.

## Passing values as parameters

The application passes values separately from the text; the query text does not change:

```yql
DECLARE $userId AS Uint64;
DECLARE $status AS Utf8;

SELECT id, name FROM users WHERE id = $userId AND status = $status;
```

The server handles calls to such a query differently:

1. **First call** — the server compiles the query and stores the result in the cache on the node.
2. **Later calls** — if the text is already in the cache, the ready result is reused: only `$userId` and `$status` change; recompilation is not required.

Only values can be passed as parameters: you cannot parameterize a table name or sort order; changing them changes the query text.

See [Query compile cache](../system-views.md#compile-cache-queries) for cache contents, and [Top queries](../system-views.md#top-queries) for compilation time when queries run (the `CompileDuration` field).

Cache size and settings, the `KeepInCache` flag, [`DECLARE`](../../yql/reference/syntax/declare.md) syntax, and passing parameters from code are described in [Parameterized queries](../../reference/ydb-sdk/parameterized_queries.md) in the {{ ydb-short-name }} SDK reference.

## See also

- [Query execution plan](plans.md)
- [Query compile cache](../system-views.md#compile-cache-queries)
- [Parameterized queries](../../reference/ydb-sdk/parameterized_queries.md) (SDK reference)
- [Executing parameterized queries](../../reference/ydb-cli/parameterized-query-execution.md) (CLI reference)
- [Application examples](../example-app/index.md#param-queries)
- [Query execution optimization overview](index.md)

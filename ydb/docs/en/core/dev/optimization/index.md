# Query execution optimization

When executing queries against a database, the SQL text is converted into a query plan suitable for distributed execution on a [cluster](../../concepts/glossary.md#cluster). The same query can often be executed in many different ways, and the [optimizer](../../concepts/query_execution/optimizer.md) in {{ ydb-short-name }} chooses the most efficient one.

Using query optimization tools in {{ ydb-short-name }}, you can:

- understand how the database plans to execute a query;
- analyze how the query was actually executed;
- identify and use opportunities to speed up execution.

{% include [tpch-dataset-note.md](_includes/tpch-dataset-note.md) %}

## How the database plans to execute a query

After compilation and optimization, a [query execution plan](plans.md) is built — a sequence of [operators](../../concepts/glossary.md#operator) that need to be executed to obtain the result. This plan can be obtained using the [`--explain`](plans.md#explain-cli) parameter of the CLI command `ydb sql` without actually processing data. This plan will reflect the query execution structure and contain estimates from the [cost-based optimizer](../../concepts/query_execution/optimizer.md), based on the existing **source data statistics**, such as the number of rows in the table and its size in bytes.

This plan is output in [JSON](https://en.wikipedia.org/wiki/JSON) format, but for studying it, it is more convenient to represent it as a visualization. The available options depend on how the query was executed (SDK, CLI, UI) — see [Query execution plan](plans.md).

## How the query was actually executed

For a more detailed and specific analysis of query execution, {{ ydb-short-name }} collects **execution statistics**:

- Duration of individual steps ([operators](../../concepts/glossary.md#operator)).
- Volumes of transferred data.
- Waits that occurred during interaction between individual [nodes](../../concepts/glossary.md#node) of the distributed system.

Each step of the plan is executed as a set of [tasks](../../concepts/glossary.md#task), in parallel on many [nodes](../../concepts/glossary.md#node). Detailed information for each individual task would take up too much space; instead, it is processed by the server and, for each set of identical [tasks](../../concepts/glossary.md#task), published in an aggregated form, enriching the plan in [JSON](https://en.wikipedia.org/wiki/JSON) format.

But even in aggregated form, **execution statistics** are still too large for independent analysis. To solve this problem, a special way of visualizing this statistics together with the plan in [SVG](https://en.wikipedia.org/wiki/SVG) format has been developed. To learn how to obtain such a plan, see [Query execution plan](plans.md).

## What you can do to speed up query execution

This section focuses on analyzing and interpreting query execution plans: it discusses ways to understand how the server plans and executes queries, as well as how to identify bottlenecks by examining the plan and related metrics. Practical aspects of speeding up work are covered by finding potential optimization points based on the analysis of this data.

## Section structure

- [Query execution plan](plans.md)
- [Optimizer hints](hints.md)
- [Parameterized queries and recompilation](parameterized-queries.md)

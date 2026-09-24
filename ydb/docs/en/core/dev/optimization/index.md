# Overview

When you run queries against the database, SQL text is turned into a query plan suitable for distributed execution on a [cluster](../../concepts/glossary.md#cluster). The same query can often be executed in many different ways, and the {{ ydb-short-name }} [optimizer](../../concepts/query_execution/optimizer.md) picks the most efficient option.

Query optimization tools in {{ ydb-short-name }} help you:

- understand how the database plans to run a query;
- analyze how the query actually ran;
- find ways to speed up execution.

## How the database plans a query

After compilation and optimization, {{ ydb-short-name }} builds a [query execution plan](plans.md) — a sequence of [operators](../../concepts/glossary.md#operator) that must run to produce the result. You can obtain logical and execution plans with CLI, SDK, or UI tools and compare them with runtime statistics. See [Using query plans for query optimization](plans.md).

## How the query actually ran

For deeper analysis, {{ ydb-short-name }} collects **execution statistics**: step durations, data volumes, and waits between [nodes](../../concepts/glossary.md#node) in the distributed system. Plan steps run as [tasks](../../concepts/glossary.md#task) in parallel on many nodes; the server aggregates statistics and attaches them to the plan. The [query plans](plans.md) article describes how to work with execution plans and statistics available in current tooling.

## Speeding up queries

This section focuses on reading and interpreting query plans: how the server plans and executes work, and how to spot bottlenecks. Tuning often starts with plan analysis; you can also use [optimizer hints](hints.md) and [parameterized queries](parameterized-queries.md) to avoid unnecessary recompilation and influence plan choice.

## Section contents

- [Using query plans for query optimization](plans.md)
- [Optimizer hints](hints.md)
- [Parameterized queries and recompilation](parameterized-queries.md)

# Information layout in the query plan

A graphical SVG query plan with metrics is obtained after the query is actually executed — see [Obtaining a graphical plan when executing a query via CLI](plans.md#svg-cli) and [in the UI](plans.md#svg-ui). The diagram columns are described below.

The images embedded in the documentation have no interactive elements — open the plan in a separate browser tab to use stage collapsing and tooltips.

## Individual regions (columns) {#regions}

Below is the plan of a simple query:


```sql
SELECT count(*) FROM lineitem
```


![Query execution plan](../../_assets/rts-count-lineitem.svg){inline=false}

The plan is displayed as a table: on the left — [stage structure](structure.md#stages), on the right — columns with metrics and charts.

| Column | Purpose | Details |
| --- | --- | --- |
| `Query - ...` | Structure of stages and [operators](../../concepts/glossary.md#operator) | [Plan structure](#structure) |
| `Rows` | Statistics for the stage's [operators](../../concepts/glossary.md#operator) | [Operator statistics](#operators) |
| `Tasks` | Number of [tasks](../../concepts/glossary.md#task) and execution progress | [Number of stage tasks and their execution progress](#progress) |
| `Statistics` | Total metrics by [stage](../../concepts/glossary.md#processing-stage) | [Metrics by stages](#statistics), [Query metrics visualization](metrics.md) |
| Timeline | Metric graphs over time | [Time graphs](#timeline), [Query metrics visualization](metrics.md) |

## `Query - ...` -- plan structure {#structure}

The leftmost column is the [plan stage structure](structure.md). Each table row corresponds to one [stage](../../concepts/glossary.md#processing-stage); the [operators](../../concepts/glossary.md#operator) within a stage are data processing steps (for details, see [Query plan structure](structure.md)).

The header starts with the common prefix `Query`, and the second part of the header depends on the type of query being executed:

- `ResultSet` (as in the example above) — queries with a selection (`SELECT`).
- `Sink` — modifying queries (`INSERT`, `UPDATE`, `DELETE`).
- `Precompute` — separately executed plan fragments, see [Composite structures](structure.md#complex).

## `Rows` -- operator statistics {#operators}

A [stage](../../concepts/glossary.md#processing-stage) consists of one or more [operators](../../concepts/glossary.md#operator). If at least one [physical operator](../../concepts/glossary.md#physical-operator) reports statistics, the `Operators` column is filled; for stages without reporting on [operators](../../concepts/glossary.md#operator), the cell is empty. The column content complements the picture of the [stage structure](structure.md) and aggregated metrics in `Stages`.

## `Tasks` -- number of stage tasks and their progress {#progress}

In {{ ydb-short-name }}, the query is executed in a distributed manner: the same [stage](../../concepts/glossary.md#processing-stage) can be processed in parallel by multiple [tasks](../../concepts/glossary.md#task) on one or more [nodes](../../concepts/glossary.md#node) of the [cluster](../../concepts/glossary.md#cluster).

The `Tasks` column shows the number of [tasks](../../concepts/glossary.md#task) scheduled for the stage. When you hover the cursor, a tooltip shows how many have already completed. On the left in the same column, progress is shown as a dashed bar.

The column background color reflects the CPU consumption intensity by the [stage](../../concepts/glossary.md#processing-stage); see [CPU consumption](metrics.md#cpu).

## `Statistics`: stage metrics {#statistics}

The `Statistics` column contains aggregated metrics for all [tasks](../../concepts/glossary.md#task) of the stage. Some metrics are described in [Query metrics visualization](metrics.md). A common property: these are calculated values at a certain point in time (usually at the end of query execution).

Identical metrics across different stages are brought to the same scale so that they can be quickly compared, for example, CPU or memory consumption. For details, see [Metric scale](metrics.md#scale).

## Timeline {#timeline}

The column conventionally named `Timeline` (this name is not displayed in the plan; time marks are shown instead) shows the same values as `Statistics`, but over the entire query execution interval. The time grid step is selected automatically. The query duration is shown in a gray rectangle in the top-right corner of the column.

## Interactive features {#interactive}

Open the query plan in a separate browser tab. For a large query, the plan takes many lines, and it is convenient to simplify it on the screen:

- Each [stage](../../concepts/glossary.md#processing-stage) has a button ![](../../_assets/rts-button-minus.svg) that collapses the subtree starting from this stage (the button changes to ![](../../_assets/rts-button-plus.svg) to expand).
- The button ![](../../_assets/rts-button-up.svg) switches the [stage](../../concepts/glossary.md#processing-stage) to `slim` mode: only one metric (`Output`) remains in height. The normal view returns with any action on the stage.
- In the top-left corner of the plan, the button ![](../../_assets/rts-button-up.svg) enables `slim` mode for all [stages](../../concepts/glossary.md#processing-stage), and the button ![](../../_assets/rts-button-down.svg) restores the full view and expands all subtrees.

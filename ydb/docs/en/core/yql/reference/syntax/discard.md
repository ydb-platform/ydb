# DISCARD

Calculates {% if select_command == "SELECT STREAM" %}[`SELECT STREAM`](select_stream.md){% else %}[`SELECT`](select/index.md){% endif %}{% if feature_mapreduce %}{% if reduce_command %}, [`{{ reduce_command }}`](reduce.md){% endif %}, or [`{{ process_command }}`](process.md){% endif %} without returning the result neither to the client or table.{% if feature_mapreduce %} You can't use it along with [INTO RESULT](into_result.md).{% endif %}

It's good to combine it with [`Ensure`](../builtins/basic.md#ensure) to check the final calculation result against the user's criteria.

{% if backend_name == "YDB" %}

A query with `DISCARD` is executed in full — with all its filters, aggregations, and `Ensure` checks — but the result set is not returned to the client. The data is not sent over the network either: the client receives only the query execution status. For large outputs, this saves a noticeable amount of traffic and memory.

In a query consisting of multiple statements, `DISCARD` applies to an individual statement:

```yql
SELECT 1;          -- returned
DISCARD SELECT 2;  -- executed, but not included in the response
SELECT 3;          -- returned
```

The client will receive two result sets — from the first and third statements.

`DISCARD` applies only to a statement as a whole and cannot be used inside expressions. In particular, it is not allowed:

* in subqueries — `SELECT * FROM (DISCARD SELECT 1)`;
* in `WHERE ... IN (...)` — `SELECT * FROM my_table WHERE Key IN (DISCARD SELECT 1)`;
* in `UNION ALL` operands — `SELECT 1 UNION ALL DISCARD SELECT 2`.

{% note info %}

`DISCARD` is supported for queries executed via the Query Service starting from {{ ydb-short-name }} version 26.2. When a query is executed via the legacy interfaces (Table Service, scan queries), `DISCARD` is ignored and the result is returned to the client — this behavior is preserved for backward compatibility.

{% endnote %}

{% endif %}

{% if select_command != true or select_command == "SELECT" %}

## Examples

```yql
DISCARD SELECT 1;
```

{% if backend_name == "YDB" %}

```yql
DISCARD SELECT Ensure(
    Data,
    Data < 1000000,
    "Value too big"
) FROM `result_table`;
```

If the `Ensure` condition is violated, the query fails with an error. Otherwise, the query completes successfully, and the client does not receive the result set that would otherwise have to be downloaded in full.

{% else %}

```yql
INSERT INTO result_table WITH TRUNCATE
SELECT * FROM
my_table
WHERE value % 2 == 0;

COMMIT;

DISCARD SELECT Ensure(
    0, -- will discard result anyway
    COUNT(*) > 1000,
    "Too small result table, got only " || CAST(COUNT(*) AS String) || " rows"
) FROM result_table;
```

{% endif %}

{% endif %}

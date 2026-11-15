### Examples

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b USING (key);
```

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b ON a.key = b.key;
```

```yql
SELECT     a.value as a_value, b.value as b_value, c.column2
FROM       a_table AS a
CROSS JOIN b_table AS b
LEFT  JOIN c_table AS c ON c.ref = a.key and c.column1 = b.value;
```

To make sure no full scan of the right joined table is required, a secondary index can be applied to the columns included in the Join condition. Accessing a secondary index should be specified explicitly in `JOIN table_name VIEW index_name AS table_alias` format.

For example, creating an index to use in the Join condition:

```yql
ALTER TABLE b_table ADD INDEX b_index_ref GLOBAL ON(ref);
```

Using the created index:

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
INNER JOIN b_table VIEW b_index_ref AS b ON a.ref = b.ref;
```

{% if feature_mapreduce %}

If the statement filters data in addition to `JOIN`, we recommend wrapping the criteria that would return `true` for most of the rows in the `LIKELY(...)` function call. If your assumption that positive values prevail for the criteria is correct, such a hint might speed up your subquery. `LIKELY` can be useful when the predicate calculation is a resource-intensive operation and JOIN significantly reduces the number of rows.

In front of any data source for `JOIN`, you can add the `ANY` keyword to suppress duplicate `JOIN` keys on the given side. In this case, only one row is left from the set of rows with the same `JOIN` key value (no matter which one, that's why the keyword is called `ANY`).
This syntax is different from that used in [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/join/), where `ANY` is placed before the `JOIN` type and applies to the right side only.

Query:

```yql
$t1 = AsList(
    AsStruct("1" AS key, "v111" AS value),
    AsStruct("2" AS key, "v121" AS value),
    AsStruct("2" AS key, "v122" AS value),
    AsStruct("3" AS key, "v131" AS value),
    AsStruct("3" AS key, "v132" AS value));

$t2 = AsList(
    AsStruct("2" AS key, "v221" AS value),
    AsStruct("2" AS key, "v222" AS value),
    AsStruct("3" AS key, "v231" AS value),
    AsStruct("3" AS key, "v232" AS value),
    AsStruct("4" AS key, "v241" AS value));

SELECT
  a.key, a.value, b.value
FROM ANY AS_TABLE($t1) AS a
JOIN ANY AS_TABLE($t2) AS b
ON a.key == b.key;
```

results in:

| a.key | a.value | b.value |
| --- | --- | --- |
| "3" | "v131" | "v231" |
| "2" | "v121" | "v221" |

and without `ANY` it would result in:

| a.key | a.value | b.value |
| --- | --- | --- |
| "3" | "v131" | "v231" |
| "3" | "v131" | "v232" |
| "3" | "v132" | "v231" |
| "3" | "v132" | "v232" |
| "2" | "v121" | "v221" |
| "2" | "v121" | "v222" |
| "2" | "v122" | "v221" |
| "2" | "v122" | "v222" |

{% endif %}

## MAX_BY and MIN_BY {#max-min-by}

Return the value of the first argument for the table row where the second argument is minimum/maximum.

You can optionally specify the third argument N that affects behavior if the table has multiple rows with the same minimum or maximum value:

* If N is omitted, the value of one of the rows is returned, and the other rows are discarded.
* If N is specified, the list is returned with all values, but their number can't exceed N. All values after the number are discarded.

When choosing N, we recommend that you don't exceed several hundreds or thousands to avoid issues with the limited memory available on {{ backend_name }} clusters.

If your task needs absolutely all values, and their number is measured in dozens of thousands or more, then instead of those aggregate functions better use `JOIN` on the source table with a subquery doing `GROUP BY + MIN/MAX` on the desired columns of this table.

{% note warning "Attention" %}

If the second argument is always NULL, the aggregation result is NULL.

{% endnote %}

When you use [aggregation factories](../../basic.md#aggregationfactory), a `Tuple` containing a value and a key is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**

```yql
SELECT
  MIN_BY(value, LENGTH(value)),
  MAX_BY(value, key, 100)
FROM my_table;
```

```yql
$min_by_factory = AggregationFactory("MIN_BY");
$max_by_factory = AggregationFactory("MAX_BY", 100);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $min_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $max_by_factory)
FROM my_table;
```


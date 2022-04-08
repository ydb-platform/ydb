## TOP and BOTTOM {#top-bottom}

Return a list of the maximum/minimum values of an expression. The first argument is an expression, the second argument limits the number of items.

**Examples**

```yql
SELECT
    TOP(key, 3),
    BOTTOM(value, 3)
FROM my_table;
```

```yql
$top_factory = AggregationFactory("TOP", 3);
$bottom_factory = AggregationFactory("BOTTOM", 3);

SELECT
    AGGREGATE_BY(key, $top_factory),
    AGGREGATE_BY(value, $bottom_factory)
FROM my_table;
```

## TOP_BY and BOTTOM_BY {#top-bottom-by}

Return a list of values of the first argument for the rows containing the maximum/minimum values of the second argument. The third argument limits the number of items in the list.

When you use [aggregation factories](../../basic.md#aggregationfactory), a `Tuple` containing a value and a key is passed as the first [AGGREGATE_BY](#aggregateby) argument. In this case, the limit for the number of items is passed by the second argument at factory creation.

**Examples**

```yql
SELECT
    TOP_BY(value, LENGTH(value), 3),
    BOTTOM_BY(value, key, 3)
FROM my_table;
```

```yql
$top_by_factory = AggregationFactory("TOP_BY", 3);
$bottom_by_factory = AggregationFactory("BOTTOM_BY", 3);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $top_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $bottom_by_factory)
FROM my_table;
```


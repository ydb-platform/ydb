### Signature


```yql
TOP(T?, limit:Uint32)->List<T>
TOP(T, limit:Uint32)->List<T>
BOTTOM(T?, limit:Uint32)->List<T>
BOTTOM(T, limit:Uint32)->List<T>
```


Return a list of the maximum/minimum values of an expression. The first argument is an expression, the second argument limits the number of items.

### Examples


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

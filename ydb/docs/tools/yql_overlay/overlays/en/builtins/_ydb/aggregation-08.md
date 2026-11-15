### Signature


```yql
SOME(T?)->T?
SOME(T)->T?
```


Get the value for an expression specified as an argument, for one of the table rows. Gives no guarantee of which row is used. It's similar to the [any()](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/any/) function in ClickHouse.

Because of no guarantee, `SOME` is computationally cheaper than [MIN / MAX](#min-max) often used in similar situations.

### Examples


```yql
SELECT
  SOME(value)
FROM my_table;
```


{% note alert %}

When the aggregate function `SOME` is called multiple times, it's **not** guaranteed that all the resulting values are taken from the same row of the source table. To get this guarantee, pack the values into any container and pass it to `SOME`. For example, in the case of a structure, you can apply [AsStruct](../basic.md#as-container)

{% endnote %}

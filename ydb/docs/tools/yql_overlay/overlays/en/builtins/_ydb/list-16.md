{% if feature_udf_noncpp %}

    * [Python UDF](../../udf/python.md), [JavaScript UDF](../../udf/javascript.md) or any other called value.

If the source list is optional, then the output list is also optional.

{% if feature_column_container_type %}

### Examples

```yql
$callable = Python::test(Callable<(Int64)->Bool>, "defMyFavouriteCrutchtest(i): return i % 2");
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf),
    ListFilter(list_column, $callable)
FROM my_table;
```

{% endif %}

{% endif %}

{% if feature_udf_noncpp and oss != true %}

    * [Python UDF](../../udf/python.md), [JavaScript UDF](../../udf/javascript.md) или любое другое вызываемое значение;

{% endif %}

Если исходный список является опциональным, то таким же является и выходной список.

### Примеры

{% if feature_column_container_type %}

```yql
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf)
FROM my_table;
```

{% endif %}

```yql
$list = AsList("a", "b", "c");

$filter = ($x) -> {
    RETURN $x == "b";
};

SELECT ListFilter($list, $filter);  -- ["b"]
```

```yql
$list = AsList(1,2,3,4);
$callable = Python::test(Callable<(Int64)->Bool>, "def test(i): return i % 2");
SELECT ListFilter($list, $callable);  -- [1,3]
```

### Сигнатура

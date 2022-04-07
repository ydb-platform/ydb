## AS {#as}

Can be used in the following scenarios:

* Adding a short name (alias) for columns or tables within the query.
* Using named arguments in function calls.
* To specify the target type in the case of explicit type casting, see [CAST](#cast).

{% if select_command != "SELECT STREAM" %}
**Examples:**

```yql
SELECT key AS k FROM my_table;
```

```yql
SELECT t.key FROM my_table AS t;
```

```yql
SELECT
    MyFunction(key, 123 AS my_optional_arg)
FROM my_table;
```

{% else %}
**Examples:**

```yql
SELECT STREAM key AS k FROM my_stream;
```

```yql
SELECT STREAM s.key FROM my_stream AS s;
```

```yql
SELECT STREAM
    MyFunction(key, 123 AS my_optional_arg)
FROM my_stream;
```

{% endif %}


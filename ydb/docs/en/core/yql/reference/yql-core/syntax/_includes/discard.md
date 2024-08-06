# DISCARD

Calculates {% if select_command == "SELECT STREAM" %}[`SELECT STREAM`](../select_stream.md){% else %}[`SELECT`](../select/index.md){% endif %}{% if feature_mapreduce %}{% if reduce_command %}, [`{{ reduce_command }}`](../reduce.md){% endif %}, or [`{{ process_command }}`](../process.md){% endif %} without returning the result neither to the client or table. {% if feature_mapreduce %}You can't use it along with [INTO RESULT](../into_result.md).{% endif %}

It's good to combine it with [`Ensure`](../../builtins/basic.md#ensure) to check the final calculation result against the user's criteria.

{% if select_command != true or select_command == "SELECT" %}
**Examples**

```yql
DISCARD SELECT 1;
```

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


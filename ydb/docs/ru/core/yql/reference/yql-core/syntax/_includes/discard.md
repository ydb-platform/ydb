# DISCARD

Вычисляет {% if select_command == "SELECT STREAM" %}[`SELECT STREAM`](../select_stream.md){% else %}[`SELECT`](../select/index.md){% endif %}{% if feature_mapreduce %}{% if reduce_command %}, [`{{ reduce_command }}`](../reduce.md){% endif %}  или [`{{ process_command }}`](../process.md){% endif %}, но не возвращает результат ни в клиент, ни в таблицу. {% if feature_mapreduce %}Не может быть задано одновременно с [INTO RESULT](../into_result.md).{% endif %}

Полезно использовать в сочетании с [`Ensure`](../../builtins/basic.md#ensure) для проверки выполнения пользовательских условий на финальный результат вычислений.

{% if select_command != true or select_command == "SELECT" %}
**Примеры**

``` yql
DISCARD SELECT 1;
```

``` yql
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
### Examples


```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```


{% if feature_window_functions %}

## SessionStart {#session-start}

No arguments. It's allowed only if there is [SessionWindow](../../syntax/select/group-by.md#session-window) in [GROUP BY](../../syntax/select/group-by.md) / [PARTITION BY](../../syntax/select/window.md#partition).
Returns the value of the `SessionWindow` key column. If `SessionWindow` has two arguments, it returns the minimum value of the first argument within the group/partition.
In the case of the expanded version `SessionWindow`, it returns the value of the second element from the tuple returned by `<calculate_lambda>`, for which the first tuple element is `True`.

{% endif %}

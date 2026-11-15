{% if feature_column_container_type %}

### Examples

```yql
SELECT ListReverse(list_column) FROM my_table;
```

{% endif %}

## ListSkip {#listskip}

Returns a copy of the list, skipping the specified number of its first elements.

The first argument specifies the source list and the second argument specifies how many elements to skip.

{% if feature_column_container_type %}

### Examples

```yql
SELECT
    ListSkip(list_column, 3)
FROM my_table;
```

{% endif %}

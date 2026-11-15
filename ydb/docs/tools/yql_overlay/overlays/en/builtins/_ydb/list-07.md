Sequentially join lists (concatenation of lists). The arguments can be lists, optional lists, and `NULL`.
The types of list items must be compatible in the case of `ListExtend` and strictly match in the case of `ListExtendStrict`.
If at least one of the lists is optional, then the result is also optional.
If at least one argument is `NULL`, then the result type is `NULL`.

{% if feature_column_container_type %}

### Examples

```yql
SELECT ListExtend(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

{% endif %}

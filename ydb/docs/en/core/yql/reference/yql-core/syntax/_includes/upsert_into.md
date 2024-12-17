# UPSERT INTO

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

{% include [ways_add_data_to_olap](../../../../_includes/ways_add_data_to_olap.md) %}

{% endnote %}

{% endif %}

UPSERT (which stands for UPDATE or INSERT) updates or inserts multiple rows to a table based on a comparison by the primary key. Missing rows are added. For the existing rows, the values of the specified columns are updated, but the values of the other columns are preserved.

{% if feature_mapreduce %}

The table is searched by name in the database specified by the [USE](../use.md) operator.

{% endif %}

{% if feature_replace %}

`UPSERT` and [`REPLACE`](../replace_into.md) are data modification operations that don't require a prefetch and run faster and cheaper than other operations because of that.
{% else %}
`UPSERT` is the only data modification operation that doesn't require prefetching and runs faster and cheaper than other operations because of that.

{% endif %}

Column mapping when using `UPSERT INTO ... SELECT` is done by names. Use `AS` to fetch a column with the desired name in `SELECT`.

## Examples

```yql
UPSERT INTO my_table
SELECT pk_column, data_column1, col24 as data_column3 FROM other_table
```

```yql
UPSERT INTO my_table ( pk_column1, pk_column2, data_column2, data_column5 )
VALUES ( 1, 10, 'Some text', Date('2021-10-07')),
       ( 2, 10, 'Some text', Date('2021-10-08'))
```


# UPSERT INTO

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

Supported only for [row-oriented](../../../concepts/datamodel/table.md#row-oriented-tables) tables. Support for [column-oriented](../../../concepts/datamodel/table.md#column-oriented-tables) tables is currently under development.

{% if oss %}

Available methods for loading data into columnar tables:

* [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/export-import/import-file.md)
* [Bulk data upsert](../../../recipes/ydb-sdk/bulk-upsert.md)
* [Yandex Data Transfer](https://yandex.cloud/ru/services/data-transfer)

{% endif %}

{% endnote %}

{% endif %}

UPSERT (which stands for UPDATE or INSERT) updates or inserts multiple rows to a table based on a comparison by the primary key. Missing rows are added. For the existing rows, the values of the specified columns are updated, but the values of the other columns are preserved.

{% if feature_mapreduce %}

The table is searched by name in the database specified by the [USE](use.md) operator.

{% endif %}

{% if feature_replace %}

`UPSERT` and [`REPLACE`](replace_into.md) are data modification operations that don't require a prefetch and run faster and cheaper than other operations because of that.
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

## RETURNING

`RETURNING` returns values:
* For new rows – all inserted values
* For updated rows – new values after the update  
This allows you to get the results of the operation immediately without a separate `SELECT` query.

## Examples

Returning all columns of the modified row

```
UPSERT INTO orders (order_id, status, amount)
VALUES (1001, 'shipped', 500)
RETURNING *;
```

Result

|order_id|status|amount|
|-|-|-|
|1001|shipped|500|

Returning specific columns

```
UPSERT INTO users (user_id, name, email)
VALUES (42, 'John Doe', 'john@example.com')
RETURNING user_id, email;
```

Result

|user_id|email|
|-|-|
|42|john@example.com|
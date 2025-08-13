# REPLACE INTO

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

Saves data to a table, overwriting the rows based on the primary key.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](use.md) operator.{% endif %} If the given primary key is missing, a new row is added to the table. If the given `PRIMARY_KEY` exists, the row is overwritten. The values of columns not involved in the operation are replaced by their default values.

{% note info %}

Unlike [`INSERT INTO`](insert_into.md) and [`UPDATE`](update.md), the queries [`UPSERT INTO`](upsert_into.md) and `REPLACE INTO` don't need to pre-fetch the data, hence they run faster.

{% endnote %}

## Examples

* Setting values for `REPLACE INTO` using `VALUES`.

  ```yql
  REPLACE INTO my_table (Key1, Key2, Value2) VALUES
      (1u, "One", 101),
      (2u, "Two", 102);
  COMMIT;
  ```

* Fetching values for `REPLACE INTO` using a `SELECT`.

  ```yql
  REPLACE INTO my_table
  SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
  FROM my_table1;
  COMMIT;
  ```

## RETURNING

`RETURNING` in the `REPLACE INTO` statement returns the values of inserted or updated rows. This allows you to get the results of the operation immediately without a separate `SELECT` query.

## Example

Returning specific columns

```
REPLACE INTO some_table (id, color, price)
VALUES
(1101, 'red', 200),
(1102, 'green', 300)
RETURNING id, price;
```

Result:

|id|price|
|-|-|
|1101|200|
|1102|300|

Returning all columns

```
REPLACE INTO some_table (id, year, color, price)
VALUES (1103, 2023, 'blue', 400)
RETURNING *;
```

Result:

|id|year|color|price|
|-|-|-|-|
|1103|2023|blue|400|

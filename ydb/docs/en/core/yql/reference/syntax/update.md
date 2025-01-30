# UPDATE

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

Updates the data in the table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](use.md) operator.{% endif %} After the `SET` keyword, enter the columns where you want to update values and the new values themselves. The list of rows is defined by the `WHERE` clause. If `WHERE` is omitted, the updates are applied to all the rows of the table.

`UPDATE` can't change the value of the primary key columns.

## Example

```yql
UPDATE my_table
SET Value1 = YQL::ToString(Value2 + 1), Value2 = Value2 - 1
WHERE Key1 > 1;
```

## UPDATE ON {#update-on}

Updates the data in the table based on the results of a subquery. The set of columns returned by the subquery must be a subset of the table's columns being updated, and all columns of the table's primary key must be present in the returned columns. The data types of the columns returned by the subquery must match the data types of the corresponding columns in the table.

The primary key value is used to search for the rows being updated. For each row found, the values of the non-key columns is replaced with the values returned in the corresponding row of the result of the subquery. The values of the table columns that are missing in the returned columns of the subquery remain unchanged.

### Example

```yql
$to_update = (
    SELECT Key, SubKey, "Updated" AS Value FROM my_table
    WHERE Key = 1
);

UPDATE my_table ON
SELECT * FROM $to_update;
```

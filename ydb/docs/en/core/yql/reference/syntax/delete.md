# DELETE FROM

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

Supported only for [row-oriented](../../../concepts/datamodel/table.md#row-oriented-tables) tables. Support for [column-oriented](../../../concepts/datamodel/table.md#column-oriented-tables) tables is currently under development.

Instead of using `DELETE FROM` to delete data from colum-oriented tables, you can use the mechanism of deleting rows by time — [TTL](../../../concepts/ttl.md). TTL can be set when [creating](create_table/index.md) the table via `CREATE TABLE` or [modified](alter_table/index.md) later via `ALTER TABLE`.

{% endnote %}

{% endif %}

Deletes rows that match the `WHERE` clause, from the table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](use.md) operator.{% endif %}

## Example

```yql
DELETE FROM my_table
WHERE Key1 == 1 AND Key2 >= "One";
```

## DELETE FROM ... ON {#delete-on}

Deletes rows based on the results of a subquery. The set of columns returned by the subquery must be a subset of the table's columns being updated, and all columns of the table's primary key must be present in the returned columns. The data types of the columns returned by the subquery must match the data types of the corresponding columns in the table.

The primary key value is used to search for rows to be deleted from the table. The presence of other (non-key) columns of the table in the output of the subquery does not affect the results of the deletion operation.

### Example

```yql
$to_delete = (
    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete" LIMIT 100
);

DELETE FROM my_table ON
SELECT * FROM $to_delete;
```

## BATCH DELETE {#batch-delete}

Deletes data from large tables that cannot be deleted using the standard `DELETE` method due to transaction limits and the risk of potential transaction locks invalidation failure. It independently applies deletions to each partition of the specified table, removing a limited number of rows per iteration (by default, 10000 rows). The query is executed in a non-transactional mode. In case of an error, changes are not rolled back. The semantics are inherited from the standard `DELETE` with the following restrictions:

* Supported only for row-oriented tables.
* The use of subqueries and multiple queries in a single expression, including `DELETE FROM ... ON`, is prohibited.
* The `RETURNING` keyword is unavailable.

### Example

```yql
BATCH DELETE FROM my_table
WHERE Key1 > 1 AND Key2 >= "One";
```

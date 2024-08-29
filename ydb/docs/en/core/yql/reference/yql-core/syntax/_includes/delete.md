# DELETE FROM

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

Instead of using `DELETE FROM` to remove data from columnar tables, you can use the mechanism of deleting rows by time — [TTL](../../../../concepts/ttl.md). TTL can be set when [creating](../create_table/index.md) the table via `CREATE TABLE` or [modified](../alter_table.md) later via `ALTER TABLE`.

{% endnote %}

{% endif %}

Deletes rows that match the `WHERE` clause, from the table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %}

**Example**

```sql
DELETE FROM my_table 
WHERE Key1 == 1 AND Key2 >= "One";
```

## DELETE FROM ... ON {#delete-on}

Deletes rows based on the results of a subquery. The set of columns returned by the subquery must be a subset of the table's columns being updated, and all columns of the table's primary key must be present in the returned columns. The data types of the columns returned by the subquery must match the data types of the corresponding columns in the table.

The primary key value is used to search for rows to be deleted from the table. The presence of other (non-key) columns of the table in the output of the subquery does not affect the results of the deletion operation.

**Example**

```sql
$to_delete = (
    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete" LIMIT 100
);

DELETE FROM my_table ON
SELECT * FROM $to_delete;
```

# DELETE FROM

Deletes rows that match the `WHERE` clause, from the table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %}

{% note info %}

The changes of the table state can't be tracked within a single transaction. If the table has already been changed, use [`DELETE ON`](#delete-on) to delete the data within the same transaction.

{% endnote %}

**Example**

```sql
DELETE FROM my_table 
WHERE Key1 == 1 AND Key2 >= "One";
COMMIT;
```

## DELETE FROM ... ON {#delete-on}

Used to delete the data, if the table has already been changed within the same transaction.

**Example**

```sql
$to_delete = (
    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete"
);

SELECT * FROM my_table;

DELETE FROM my_table ON 
SELECT * FROM $to_delete;
COMMIT;
```


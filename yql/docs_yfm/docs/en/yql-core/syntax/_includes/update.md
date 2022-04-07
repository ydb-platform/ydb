# UPDATE

Updates the data in the table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %} After the `SET` keyword, enter the columns where you want to update values and the new values themselves. The list of rows is defined by the `WHERE` clause. If `WHERE` is omitted, the updates are applied to all the rows of the table.

`UPDATE` can't change the value of `PRIMARY_KEY`.

{% note info %}

The table state changes can't be tracked within a single transaction. If the table has already been changed, use [`UPDATE ON`](#update-on) to update the data within the same transaction.

{% endnote %}

**Example**

```sql
UPDATE my_table
SET Value1 = YQL::ToString(Value2 + 1), Value2 = Value2 - 1
WHERE Key1 > 1;
```

## UPDATE ON {#update-on}

Used to update the data within a same transaction, if the table has already been changed.

**Example**

```sql
$to_update = (
    SELECT Key, SubKey, "Updated" AS Value FROM my_table
    WHERE Key = 1
);

SELECT * FROM my_table;

UPDATE my_table ON
SELECT * FROM $to_update;
```


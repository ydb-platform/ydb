# Creating a table filled with query results

{% include [not_allow_for_oltp](../../../../_includes/not_allow_for_oltp_note.md) %}

`CREATE TABLE AS` creates a new table {% if concept_table %}[table]({{ concept_table }}){% else %}table{% endif %} filled with data from query results.

```yql
CREATE TABLE table_name (
    PRIMARY KEY ( column, ... )
)
WITH ( key = value, ... )
AS SELECT ...
```

Names and types of columns will correspond to the `SELECT` results.
[Non-optional](../../types/optional.md) columns will also have the `NOT NULL` constraint.


When creating a table using `CREATE TABLE AS`, it is not possible to specify column names (column names of the created table will be derived from the query result), [secondary indexes](secondary_index.md), [vector indexes](vector_index.md), [fulltext indexes](fulltext_index.md), or [column groups](family.md). All of those can be changed after the table has been created using [`ALTER TABLE`](../alter_table/index.md). [Additional parameters](with.md) are also supported.



## Considerations

{% note warning %}

Rows are overwritten, similar to using [`REPLACE INTO`](../replace_into.md ), but the order in which rows are written is unpredictable.

If `SELECT` returns two or more rows with the same primary key value, after the `CREATE TABLE AS` is executed, there will only be one row with that primary key value in the created table. Which record from the `SELECT` was written to the table is undetermined.

{% endnote %}


* `CREATE TABLE AS` is supported only for [implicit transaction control mode](../../../../ concepts/transactions.md#implicit). When the table appears at the specified path it's already filled.

* `CREATE TABLE AS` can only be a sigle [DML](https://en.wikipedia.org/wiki/Data_manipulation_language)/[DDL](https://en.wikipedia.org/wiki/Data_definition_language) statement in a query. It's possible to use [PRAGMA](../pragma.md), [DECLARE](../declare.md) or [named expressions](../expressions.md#named-nodes) in the same query.

* `CREATE TABLE AS` doesn't cause lock conflicts with other transactions. It doesn't use locks. Reads use a consistent snapshot. Moving or splitting [tablets](../../../../concepts/glossary.md#tablet) doesn't cause errors.

* `CREATE TABLE AS` allows using [column-oriented tables](../../../../concepts/glossary.md#column-oriented-table) and [row-oriented tables](../../../../concepts/glossary.md#row-oriented-table) in the same query.

* `CREATE TABLE AS` creates a temporary table and moves it to the specified location after filling that table. If there was an error during the `CREATE TABLE AS` execution, it's possible that the temporary table will not be deleted immediately, but it will remain for some short period of time.

## Examples

* Creating a column-oriented table from the query results

    ```yql
    CREATE TABLE my_table (
        PRIMARY KEY (key1, key2)
    ) WITH (
        STORE=COLUMN
    ) AS SELECT 
        key AS key1,
        Unwrap(other_key) AS key2,
        value,
        String::Contains(value, "test") AS has_test
    FROM other_table;
    ```
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

The `CREATE TABLE AS` syntax allows you to specify only the primary key and `WITH` parameters, so when creating a table, specifying column names, [secondary indexes](secondary_index.md), [vector indexes](vector_index.md), [full-text indexes](fulltext_index.md), [local bloom indexes](bloom_skip_index.md), [local min_max indexes](min_max_index.md), and [column groups](family.md) is not supported. The column names and data types of the new table are automatically inherited from the result set of the SELECT query. You can change all of the above using [`ALTER TABLE`](../alter_table/index.md) after creating the table. Additionally, [additional parameters](with.md) are supported.

## Considerations

{% note warning %}

Rows are overwritten, similar to using [`REPLACE INTO`](../replace_into.md), but the order in which rows are written is unpredictable.

If `SELECT` returns two or more rows with the same primary key value, after the `CREATE TABLE AS` is executed, there will only be one row with that primary key value in the created table. Which record from the `SELECT` was written to the table is undetermined.

{% endnote %}

* `CREATE TABLE AS` is supported only in the [implicit transaction control](../../../../concepts/transactions.md#implicit) mode. The table will appear at the specified path already populated.
* `CREATE TABLE AS` can only be a single [DML](https://en.wikipedia.org/wiki/Data_manipulation_language)/[DDL](https://en.wikipedia.org/wiki/Data_definition_language) statement in a query. It's possible to use [PRAGMA](../pragma.md), [DECLARE](../declare.md) or [named expressions](../expressions.md#named-nodes) in the same query.
* `CREATE TABLE AS` doesn't cause lock conflicts with other transactions. It doesn't use locks. Reads use a consistent snapshot. Moving or splitting [tablets](../../../../concepts/glossary.md#tablet) doesn't cause errors.
* `CREATE TABLE AS` allows using [column-oriented tables](../../../../concepts/glossary.md#column-oriented-table) and [row-oriented tables](../../../../concepts/glossary.md#row-oriented-table) in the same query.
* `CREATE TABLE AS` creates a table in the temporary directory `.tmp/sessions`, and after successful data write moves it to the specified location. If the operation is interrupted due to an error, the temporary table is not deleted immediately but remains in the system for some time.

## Examples

* Creating a columnar table from query results


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

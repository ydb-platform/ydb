# Secondary indexes

[An index]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Индекс_(базы_данных)){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Database_index){% endif %} is an auxiliary data structure that lets you find data that meets certain criteria in a database, without having to search every row in a DB table, as well as obtain sorted samples without performing actual sorting that requires processing a complete set of all sorted data.

Data in YDB tables is always indexed by primary key. This means that fetching any record from a table with the specified values of primary key fields will always take the minimum fixed time, regardless of the total number of records in the table. A primary key index also lets you get any consecutive range of records in ascending or descending order of primary key values. The execution time of this operation will only depend on the number of records received, regardless of the total number of records in the table.

To use similar features for any table fields or combinations thereof, additional indexes called **secondary indexes** can be built based on them.

Transactional systems use indexes to avoid performance degradation and an increase in the query execution cost if the amount of stored data grows.

This article describes the basic operations with secondary indexes and provides links to detailed information on each operation. For information about different types of secondary indexes and their specifics, see the [Secondary indexes](../../concepts/secondary_indexes.md) article in the "Concepts" section.

## Creating secondary indexes {#create}

A secondary index is a data schema object that can be set when creating a table with the [YQL `CREATE TABLE`](../../yql/reference/syntax/create_table.md) statement or added to it later with the [YQL `ALTER TABLE`](../../yql/reference/syntax/alter_table.md) statement.

The [`table index add`](../../reference/ydb-cli/commands/secondary_index.md#add) command for creating an index is supported in the YDB CLI.

Since an index contains its own data that is derived from table data, an initial index build operation is performed when creating an index in an existing data table. This may take a long time. This operation is run in the background and is non-blocking for the table. However, you can't use a new index until its build completes.

Indexes can only be used in the order of their fields. If there are two index fields, `a` and `b`, this index can be effectively used for queries like:

* `where a = $var1 and b = $var2`;
* `where a = $var1`;
* `where a > $var1`, and other comparison operators;
* `where a = $var1 and b > $var2`, and any other comparison operators, but the first field must be checked for equality.

At the same time, you can't use this index for the following queries:

* `where b = $var1`;
* `where a > $var1 and b > $var2`, to be more precise, this entry will be equivalent to `where a > $var1` in terms of using the index;
* `where b > $var1`.

Considering the above specifics, it's useless to try to index all possible combinations of table columns in advance to speed up the execution of any query. An index is always a trade-off between the speed of searching and writing data and the storage space this data takes. Indexes are created for specific selects and criteria for a search that an application will make in the database.

## Using secondary indexes when making a Select {#use}

To access a table by secondary index, its name must be explicitly specified in the `view` section after the table name as described in the article about the YQL [`SELECT`](../../yql/reference/syntax/select#secondary_index) statement. For example, to make a Select from the `orders` table for the customer with the specified ID (`id_customer`), run a query like this:

```sql
DECLARE $customer_id AS Uint64;
SELECT *
FROM   orders as o view idx_customer
WHERE  o.id_customer = $customer_id
```

where`idx_customer` is the name of a secondary index on the `orders` table with the `id_customer` field specified first.

If the `view` section is omitted, a full scan of the `orders` table is performed for making this query.

In transactional applications, such information requests are executed using paginated output. This helps avoid an increase in costs and execution time if the number of records that meet the filtering criteria grows. The approach to making [queries with pagination](../paging.md) that is described using a primary key as an example is also applicable to columns included in a secondary index.

## Checking the cost of a query {#cost}

Any query in a transactional application should be checked in terms of how many I/O operations it performed in the database and how much CPU it used to run. You should also make sure that these metrics do not grow indefinitely with the growth of the DB size. After making every query, YDB returns statistics that you can use to analyze the necessary parameters.

If you use the YDB CLI, select the `--stats` option to enable statistics output after executing the `yql` command. All YDB SDKs also contain structures that provide statistics after making queries. If you run queries in the UI, there is also a tab with statistics next to the results tab.

## Updating data using a secondary index {#update}

YQL statements used for making changes to records ([`UPDATE`](../../yql/reference/syntax/update.md), [`UPSERT`](../../yql/reference/syntax/upsert_into.md), and [`REPLACE`](../../yql/reference/syntax/replace_into.md)) don't let you indicate that a secondary index should be used to search for data, so an attempt to run an `UPDATE ... WHERE indexed_field = $value` will result in a full scan of the table. To avoid this, you can first make a `SELECT` by index to get the primary key value and then an `UPDATE` by primary key. You can also use `UPDATE ON`.

To update data in `table1`, run the query:

```sql
$to_update = (
    SELECT pk_field, field1 = $f1, field2 = $f2, ...
    FROM   table1 view idx_field3
    WHERE  field3 = $f3)

UPDATE table1 ON SELECT * FROM $to_update
```

## Deleting data using a secondary index {#delete}

To delete data by secondary index, use `SELECT` with a predicate by secondary index and then call `DELETE ON`.

To delete all data about series with zero views from the `series` table, run the query:

```sql
DELETE FROM series ON
SELECT series_id,
FROM series view views_index
WHERE views = 0;
```

## Performance of writing data to tables with secondary indexes {#write_performance}

Additional data structures are needed for secondary indexes to work. The support of these structures leads to an increase in the cost of table data update operations.

With synchronous index updates, a transaction is only committed after writing all the necessary data, both in the table and in synchronous indexes. As a result, it takes longer to execute transactions and makes it necessary to use [distributed transactions](../../concepts/transactions#distributed-tx) even if adding and updating records in the only partition.

Asynchronously updated indexes can still use single-shard transactions. However, they only guarantee eventual consistency and still generate load on the database.


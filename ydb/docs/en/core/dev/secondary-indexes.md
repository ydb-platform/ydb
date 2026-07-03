# Secondary indexes

{% include [not_allow_for_olap](../_includes/not_allow_for_olap_note.md) %}

[Indexes](https://en.wikipedia.org/wiki/Database_index) are auxiliary structures in databases that allow finding data matching a certain criterion without the need for a full scan, and also obtaining sorted samples without performing actual sorting that would require processing the entire set of all sorted data.

Data in row-oriented {{ ydb-short-name }} tables is always indexed by the primary key. This means that retrieving any record from a row-oriented table with given values of the fields that make up the primary key will always take a minimum fixed time, regardless of the total number of records in the row-oriented table. Also, having an index on the primary key allows you to retrieve any sequential range of records in ascending or descending order of the primary key. The execution time of such an operation will depend only on the number of records retrieved, but not on the total number of records in the row-oriented table. For primary key design recommendations, see [{#T}](primary-key/row-oriented.md).

To use similar capabilities for any fields or combinations of fields in a row-oriented table, additional indexes called **secondary indexes** can be built on them.

In transactional systems, using indexes helps reduce or eliminate performance degradation and increased query execution costs as the volume of stored data grows.

This article describes the main operations for working with secondary indexes and provides links to detailed materials for each operation. Information about different types of secondary indexes and their features can be found in the [Secondary indexes](../concepts/query_execution/secondary_indexes.md) article in the 'Concepts' section.

## Creating secondary indexes {#create}

A secondary index is a schema object and can be defined when creating a table using the [YQL `CREATE TABLE` command](../yql/reference/syntax/create_table/index.md), or added to it later using the [YQL `ALTER TABLE` command](../yql/reference/syntax/alter_table/index.md).

The [create index `table index add` command](../reference/ydb-cli/commands/secondary_index.md#add) is supported in {{ ydb-short-name }} CLI.

Since an index contains its own data derived from the table data, when creating an index on an existing row-oriented table with data, an initial index build operation will be performed, which may take a long time. This operation runs in the background, does not block table operations, but until the build is complete, the new index cannot be used.

An index can only be used in the order of the fields included in it. If an index has two fields `a` and `b`, then such an index can be effectively used for queries of the form:

* `WHERE a = $var1 AND b = $var2`
* `WHERE a = $var1`
* `WHERE a > $var1`, as well as other comparison operators
* `WHERE a = $var1 AND b > $var2`, as well as any other comparison operators, but the first field must be checked for equality

However, such an index cannot be used for the following queries:

* `WHERE b = $var1`
* `WHERE a > $var1 AND b > $var2`, more precisely, this record will be equivalent to `WHERE a > $var1` from the perspective of index usage
* `WHERE b > $var1`

To efficiently limit query results using the [`LIMIT` command](../yql/reference/syntax/select/limit_offset.md), you must use single-direction sorting in the [`ORDER BY` command](../yql/reference/syntax/select/order_by.md):

* `WHERE a = $var1 AND b > $var2 ORDER BY a, b DESC LIMIT 1` — all index rows matching the filter conditions will be read;
* `WHERE a = $var1 AND b > $var2 ORDER BY a DESC, b DESC LIMIT 1` — only one row will be read.

Given the above feature, it is useless to try to index all possible combinations of columns in a table in advance in the hope of fast execution of any queries. An index is always a compromise between search speed and write speed, as well as the storage space occupied by data. Indexes are created for specific queries and search conditions that the application will use in the database.

## Using secondary indexes for data retrieval {#use}

To access a row-oriented table by a secondary index, its name must be explicitly specified in the `VIEW` section after the table name, as described in the article about the [YQL `SELECT` command](../yql/reference/syntax/select#secondary_index). For example, to retrieve from the Orders row-oriented table (`orders`) a selection of orders for a customer with a given ID (`id_customer`), the query would look like this:


```yql
DECLARE $customer_id AS Uint64;
SELECT *
FROM   orders VIEW idx_customer AS o
WHERE  o.id_customer = $customer_id
```

, where `idx_customer` is the name of the secondary index on row-oriented table `orders`, with field `id_customer` specified first in it.

Without specifying the `VIEW` section, the `orders` row-oriented table will be fully scanned to execute such a query.

In transactional applications, such informational queries are executed using paginated data output, which eliminates the increase in cost and execution time as the number of records matching the filter conditions grows. The approach to writing [paginated queries](../dev/paging.md) described using the primary key example is also applicable to columns included in a secondary index.

An experimental feature for automatic selection of a secondary index for use in a query is also implemented. The selection algorithm is currently rule-based and uses only the query text to automatically select a secondary index.

### Automatic index usage in selects

Explicitly specifying the `VIEW` section takes precedence over the optimizer's decision to use secondary indexes. That is, the query


```yql
SELECT * FROM `Table` VIEW Index
```


will definitely perform a select using the `Index` index.

To explicitly specify reading using the primary key, use the following construct:


```yql
SELECT * FROM `Table` VIEW PRIMARY KEY
```


#### Criteria for selecting a secondary index

The selection of the index used for reading occurs during query optimization when determining the ranges of rows to be read (predicate pushdown). Indexes, like the main table, are a set of rows ordered by a set of key columns.

The choice between reading using an index and reading using the primary key is based on the following metrics:

1. Need for additional reads from the main table. If the index contains all the columns required for the query, no additional reads are needed.
2. Length of the point prefix of the predicate for the key of the corresponding table. That is, the predicate restricts a certain set of columns that are the first components of the key with point conditions: `=`, `IN`, `IS NULL`. Here, priority is given to indexes for which all indexed columns are fixed, or to the main table if the primary key is entirely point-based.
3. Number of columns used in the range boundaries for reading. In the following query to the Table table with primary key (Key1, Key2, Key3)


```yql
SELECT * FROM `Table` WHERE (Key1, Key2, Key3) < ($param1, $param2, $param3) AND (Key1, Key2) > ($param4, $param5)
```


reading will be performed in the range `(($param4, $param5), ($param1, $param2, $param3))` and thus the number of columns used will be 3. Similarly, here preference is given to indexes for which all indexed columns are used.

Reading methods are ranked according to criterion 2, with criterion 3 used for tie-breaking, and criterion 1 is additionally considered.

#### Examples of automatic index selection


```yql
CREATE TABLE `Table` (
     Key Int32,
     SubKey1 Int32,
     SubKey2 String,
     Value1 String,
     Value2 String,
     PRIMARY KEY (Key, SubKey1, SubKey2),
     INDEX Index12 GLOBAL ON (SubKey1, SubKey2),
     INDEX Index21 GLOBAL ON (SubKey2, Value1),
     INDEX Index212 GLOBAL ON (SubKey2) COVER (Value2)
);

```


`SELECT * FROM Table WHERE SubKey1 = $p1 and SubKey2 > $p2` — `Index12` will be used. Range expression — `(($p1; $p2), ($p1)]`. Point prefix length for `Index12` is 1, for other indexes — 0.

`SELECT * FROM Table WHERE Key = $p1 and SubKey1 = $p2 And SubKey2 = $p2` — the index will not be used. When selecting a scan of the main table, all 3 columns `[Key, Fk1, Fk2]` are used, point prefix length is 3.

`SELECT * FROM Table WHERE Key = $p1 and SubKey2 = $p2` — secondary indexes will not be used. When selecting any secondary index, 1 column is used, and the point prefix is also no more than 1 for any index selection option.

`SELECT * FROM Table WHERE Key >= $p1 and SubKey1 = $p2 And SubKey2 = $p3` — Index12 should be selected, because when it is selected, in the resulting range `[[Fk1; Fk2; Key], [Fk1; Fk2])`, the point prefix length will be 2, and 3 columns will be used.

`SELECT * FROM Table WHERE Key = 2 and SubKey2 = 3` — secondary indexes should not be used. In the case of reading by `PK` and when using any of the secondary indexes, the point prefix consists of no more than one column. Also, no more than one column is used.

`SELECT * FROM Table WHERE SubKey1 > 2` — `Index12` should be selected. Only when using `Index12` will there be a non-trivial range for reading.

`SELECT * FROM Table WHERE SubKey2 = 2` — Any of `Index21` and `Index212` may be selected. When using the aforementioned indexes, the point prefix length will be 1. The number of columns used is also maximized when selecting `Index21` and `Index212`.

`SELECT Value2 FROM Table WHERE SubKey2 = 2` — Index212 should be selected. When using Index21 and Index212, the point prefix length will be 1, but when using Index212, no read from the main table is needed.

`SELECT * FROM Table WHERE SubKey2 > 2` — `Index21` or `Index212` will be used, since the read range is nontrivial only when they are used.

`SELECT * FROM Table WHERE SubKey1 = 2` — `Index12` will be used, since when it is used the point prefix length will be 1, and 0 in other cases.

## Checking query cost {#cost}

Any query in a transactional application must be checked in terms of how many I/O operations it performed in the database and how much CPU was spent on its execution. It is also necessary to ensure that these numbers do not grow indefinitely as the database volume increases. In {{ ydb-short-name }}, after each query execution, statistics are returned containing the information needed for analysis.

When using {{ ydb-short-name }} CLI, the output of statistics after executing the `yql` command is enabled by the `--stats` option. All {{ ydb-short-name }} SDKs also contain structures that include statistics after query execution. When executing queries in the UI, next to the results tab there is also a statistics tab.

## Updating data using a secondary index {#update}

YQL commands for modifying records ( [`UPDATE`](../yql/reference/syntax/update.md), [`UPSERT`](../yql/reference/syntax/upsert_into.md), [`REPLACE`](../yql/reference/syntax/replace_into.md)) do not allow specifying the use of a secondary index for data search, so attempting to execute `UPDATE ... WHERE indexed_field = $value` will result in a full scan of the row-oriented table. To avoid this, you can first execute `SELECT` on the index to obtain the primary key value, and then execute `UPDATE` on the primary key. You can also use the `UPDATE ON` statement.

To update data in the `table1` row-oriented table, execute the query:


```yql
$to_update = (
    SELECT pk_field, $f1 AS field1, $f2 AS field2, ...
    FROM   table1 VIEW idx_field3
    WHERE  field3 = $f3)

UPDATE table1 ON SELECT * FROM $to_update
```


{% note info %}

Currently, data updates are only possible when using a synchronous secondary index. This is because data modification is only possible in [Serializable](../concepts/transactions.md#modes) transactions, whose guarantees are violated when using asynchronous indexes.

{% endnote %}

## Deleting data using a secondary index {#delete}

To delete data by a secondary index, use `SELECT` with a predicate on the secondary index, and then call the `DELETE ON` statement.

To delete all data about series with zero views in the `series` row-oriented table, execute the query:


```yql
DELETE FROM series ON
SELECT series_id
FROM series VIEW views_index
WHERE views = 0;
```


{% note info %}

Currently, data deletion is only possible when using a synchronous secondary index. This is because deletion is only possible in [Serializable](../concepts/transactions.md#modes) transactions, whose guarantees are violated when using asynchronous indexes.

{% endnote %}

## Atomic replacement of a secondary index {#atomic-index-replacement}

An existing secondary index can be replaced atomically. This can be useful, for example, for replacing an index with a [covering](../concepts/query_execution/secondary_indexes.md#covering) one. For running applications, this operation is transparent — at the moment of index replacement, compiled queries will be invalidated.

You can atomically replace an existing index using the {{ ydb-short-name }} CLI command [{{ ydb-cli }} table index rename](../reference/ydb-cli/commands/secondary_index.md#rename) with the `--replace` parameter.

## Write performance in row-oriented tables with secondary indexes {#write_performance}

Secondary indexes require additional data structures. Maintaining these structures increases the cost of data modification operations in row-oriented tables.

With synchronous index updates, the transaction is committed only after all necessary data is written, both in the row-oriented table and in the synchronous indexes. This leads to both increased execution time and the need to use [distributed transactions](../concepts/transactions#distributed-tx) even when adding or modifying records in a single partition.

Asynchronously updated indexes retain the ability to use single-shard transactions, but only guarantee eventual consistency, and still create load on the database.

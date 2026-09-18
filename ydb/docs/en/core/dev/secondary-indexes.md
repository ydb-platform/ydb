# Secondary indexes

{% include [not_allow_for_olap](../_includes/not_allow_for_olap_note_main.md) %}

[Indexes](https://en.wikipedia.org/wiki/Database_index) are auxiliary structures in databases that let you find data matching a specific criterion without having to scan all of it, and also get sorted samples without performing an actual sort that would require processing the entire set of data being sorted.

Data in row tables {{ ydb-short-name }} is always indexed by the primary key. This means that fetching any record from a row table with given values of the fields that make up the primary key will always take a minimal fixed amount of time, regardless of the total number of records in the row table. Also, having an index on the primary key lets you get any sequential range of records in ascending or descending order of the primary key. The execution time of such an operation depends only on the number of records being fetched, not on the total number of records in the row table. See the section [{#T}](primary-key/row-oriented.md) for primary key design recommendations.

To use similar capabilities for any fields or combinations of fields in a row table, additional indexes called **secondary indexes** can be built on them.

In transactional systems, using indexes helps reduce or eliminate performance degradation and the increased cost of query execution as the volume of stored data grows.

This article describes the main operations for working with secondary indexes and provides links to detailed materials for each operation. Information about different types of secondary indexes and their structure is in the article [Secondary indexes](../concepts/query_execution/secondary_indexes.md) of the "Concepts" section.

## Creating secondary indexes {#create}

A secondary index is a data schema object and can be defined when creating a table [using the YQL `CREATE TABLE` command](../yql/reference/syntax/create_table/index.md), or added to it later [using the YQL `ALTER TABLE` command](../yql/reference/syntax/alter_table/index.md).

The [index creation command `table index add`](../reference/ydb-cli/commands/secondary_index.md#add) is supported in the {{ ydb-short-name }} CLI.

Since an index contains its own data derived from the data in the row table, creating an index on an existing table with data will trigger an initial index build operation, which may take a long time. This operation runs in the background, does not block table operations, but the new index cannot be used until the build is complete.

An index can only be used in the order of the fields included in it. If an index has two fields `a` and `b`, then such an index can be effectively used for queries of the form:

* `WHERE a = $var1 AND b = $var2`
* `WHERE a = $var1`
* `WHERE a > $var1`, as well as other comparison operators
* `WHERE a = $var1 AND b > $var2`, as well as any other comparison operators, but the first field must be checked for equality.

However, such an index cannot be used for the following queries:

* `WHERE b = $var1`
* `WHERE a > $var1 AND b > $var2`, more precisely, this record will be equivalent to `WHERE a > $var1` in terms of index usage
* `WHERE b > $var1`.

To effectively limit the query result using [the `LIMIT` command](../yql/reference/syntax/select/limit_offset.md), you need to use one-way sorting in [the `ORDER BY` command](../yql/reference/syntax/select/order_by.md):

* `WHERE a = $var1 AND b > $var2 ORDER BY a, b DESC LIMIT 1`, all index rows matching the filter conditions will be read.
* `WHERE a = $var1 AND b > $var2 ORDER BY a DESC, b DESC LIMIT 1`, only one row will be read.

Given the above feature, it is pointless to try to index all possible column combinations in a table in advance, expecting fast execution of any queries. An index is always a trade-off between search and write speed, as well as the storage space occupied by data. Indexes are created for specific queries and search conditions that the application will perform in the database.

## Using secondary indexes for data selection {#use}

When selecting data from a row table, {{ ydb-short-name }} provides two ways to use secondary indexes:

1. **Explicit index specification** — the name of the secondary index is specified in the `VIEW` section after the table name, as described in the article about [the `SELECT` command](../yql/reference/syntax/select#secondary_index) YQL. For example, to get from the row table Orders (`orders`) a selection of orders for a client with a given ID (`id_customer`), the query will look as follows:


   ```yql
   DECLARE $customer_id AS Uint64;
   SELECT *
   FROM   orders VIEW idx_customer AS o
   WHERE  o.id_customer = $customer_id
   ```


   , where `idx_customer` is the name of the secondary index on the row table `orders`, with the field `id_customer` specified first.
2. **Automatic index selection by the query optimizer** — if the `VIEW` section is not specified, the optimizer can independently decide to use one or another secondary index based on the query text. A detailed description of the selection criteria is given below.

{% note warning %}

If you have queries without an explicit index specification (without the `VIEW` section), then when a new index is added to a table, such queries may start using it automatically. This can lead to unexpected changes in the query plan and execution time. To avoid such surprises, it is recommended to:

* Explicitly specify in queries which index should be used via the `VIEW` section.
* Based on the secondary index selection criteria, anticipate in advance whether adding a new secondary index can affect existing queries.

{% endnote %}

### Automatic index usage in queries

Explicitly specifying the `VIEW` section takes precedence over the optimizer's decision to use secondary indexes. That is, the query


```yql
SELECT * FROM `Table` VIEW Index
```


will definitely fetch data using the `Index` index.

To explicitly specify reading using the primary key, use the following construct:


```yql
SELECT * FROM `Table` VIEW PRIMARY KEY
```


#### Secondary index selection criteria

The index used for reading is selected during query optimization when determining the ranges of rows to read (predicate pushdown). Indexes, like the main table, are a set of rows ordered by a set of key columns.

The choice between reading using an index and reading using the primary key is made based on the following metrics:

1. The need for additional reads from the main table. If the index contains all the columns needed for the query, no additional reads are required.
2. The length of the point prefix of the predicate for the key of the corresponding table. That is, the predicate restricts a certain set of columns that are the first components of the key with point conditions: `=`, `IN`, `IS NULL`. Here, priority is given to indexes for which all indexed columns are fixed, or to the main table if the primary key is entirely point-based.
3. The number of columns used in the range boundaries for reading. In the following query to the Table table with the primary key (Key1, Key2, Key3)


```yql
SELECT * FROM `Table` WHERE (Key1, Key2, Key3) < ($param1, $param2, $param3) AND (Key1, Key2) > ($param4, $param5)
```


reading will be performed in the range `(($param4, $param5), ($param1, $param2, $param3))` and thus the number of used columns will be 3. Similarly to criterion 2, here preference is given to indexes for which all indexed columns are used.

Reading methods are ranked among themselves according to criterion 2, with criterion 3 used in case of equality, and criterion 1 additionally taken into account.

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


`SELECT * FROM Table WHERE SubKey1 = $p1 and SubKey2 > $p2`: `Index12` will be used. The range expression is `(($p1; $p2), ($p1)]`. The point prefix length for `Index12` is 1, for the other indexes — 0.

`SELECT * FROM Table WHERE Key = $p1 and SubKey1 = $p2 And SubKey2 = $p2`: the index will not be used. When selecting a scan of the main table, all 3 columns `[Key, Fk1, Fk2]` are used, the point prefix length is 3.`

`SELECT * FROM Table WHERE Key = $p1 and SubKey2 = $p2`: secondary indexes will not be used. When selecting any secondary index, 1 column is used, and the point prefix is also no more than 1 for any index selection option.

`SELECT * FROM Table WHERE Key >= $p1 and SubKey1 = $p2 And SubKey2 = $p3`: Index12 should be selected, because when it is selected, in the resulting range `[[Fk1; Fk2; Key], [Fk1; Fk2])` the point prefix length will be 2, and 3 columns will be used.

`SELECT * FROM Table WHERE Key = 2 and SubKey2 = 3` — secondary indexes must not be used. When reading by `PK` and using any of the secondary indexes, the point prefix consists of at most one column. Also, at most one column is used.

`SELECT * FROM Table WHERE SubKey1 > 2` — `Index12` must be selected. Only when using `Index12` will there be a non-trivial read range.

`SELECT * FROM Table WHERE SubKey2 = 2` — Any of `Index21` and `Index212` can be selected. When using the aforementioned indexes, the point prefix length will be 1. The number of used columns is also maximized when selecting `Index21` and `Index212`

`SELECT Value2 FROM Table WHERE SubKey2 = 2` — Index212 must be selected. When using Index21 and Index212, the point prefix length will be 1, but when using Index212, there is no need to read the main table.

`SELECT * FROM Table WHERE SubKey2 > 2` — `Index21` or `Index212` will be used, since the read range is non-trivial only when they are used.

`SELECT * FROM Table WHERE SubKey1 = 2` — `Index12` will be used, since when it is used the point prefix length will be 1, and in other cases 0.

## Checking query cost {#cost}

Any query in a transactional application must be checked in terms of how many I/O operations it performed in the database and how much CPU was spent on its execution. You also need to ensure that these numbers do not grow indefinitely as the database size grows. In {{ ydb-short-name }}, after each query is executed, statistics containing the information needed for analysis are returned.

When using the {{ ydb-short-name }} CLI, the output of statistics after executing the `yql` command is enabled by the `--stats` option. All {{ ydb-short-name }} SDKs also contain structures that contain statistics after query execution. When executing queries in the UI, next to the results tab there is also a statistics tab.

## Updating data using a secondary index {#update}

YQL data modification commands ([`UPDATE`](../yql/reference/syntax/update.md), [`UPSERT`](../yql/reference/syntax/upsert_into.md), [`REPLACE`](../yql/reference/syntax/replace_into.md)) do not allow specifying the use of a secondary index for data search, so attempting to execute `UPDATE ... WHERE indexed_field = $value` will result in a full scan of the row table. To avoid this, you can first execute `SELECT` on the index to obtain the primary key value, and then execute `UPDATE` using the primary key. You can also use the `UPDATE ON` statement.

To update data in the `table1` row table, run the query:


```yql
$to_update = (
    SELECT pk_field, $f1 AS field1, $f2 AS field2, ...
    FROM   table1 VIEW idx_field3
    WHERE  field3 = $f3)

UPDATE table1 ON SELECT * FROM $to_update
```


{% note info %}

Currently, data updates are only possible using a synchronous secondary index. This is because data modification is only possible in [Serializable](../concepts/transactions.md#modes) transactions, whose guarantees are violated when using asynchronous indexes.

{% endnote %}

## Deleting data using a secondary index {#delete}

To delete data by a secondary index, use `SELECT` with a predicate on the secondary index, and then call the `DELETE ON` statement.

To delete all data about series with zero views in the `series` row table, run the query:


```yql
DELETE FROM series ON
SELECT series_id
FROM series VIEW views_index
WHERE views = 0;
```


{% note info %}

Currently, data deletion is only possible using a synchronous secondary index. This is because deletion is only possible in [Serializable](../concepts/transactions.md#modes) transactions, whose guarantees are violated when using asynchronous indexes.

{% endnote %}

## Atomic replacement of a secondary index {#atomic-index-replacement}

An existing secondary index can be replaced atomically. This can be useful, for example, for replacing an index with a [covering](../concepts/query_execution/secondary_indexes.md#covering) one. For running applications, this operation is transparent — at the moment of index replacement, compiled queries will be invalidated.

You can atomically replace an existing index using the {{ ydb-short-name }} CLI [{{ ydb-cli }} table index rename](../reference/ydb-cli/commands/secondary_index.md#rename) command with the `--replace` parameter.

## Write performance in row tables with secondary indexes {#write_performance}

Secondary indexes require additional data structures. Supporting these structures increases the cost of data modification operations in row tables.

With synchronous index updates, a transaction is committed only after all required data is written, both in the row table and in the synchronous indexes. This leads both to increased execution time and to the need to use [distributed transactions](../concepts/transactions#distributed-tx) even when adding or modifying records in a single partition.

Asynchronously updated indexes retain the ability to use single-shard transactions, but only guarantee eventual consistency, and still create load on the database.

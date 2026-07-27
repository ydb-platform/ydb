# Secondary indexes

[Indexes](https://en.wikipedia.org/wiki/Database_index) are auxiliary structures in databases: they allow finding data matching a certain criterion without having to scan all data, and also obtaining sorted samples without performing actual sorting that requires processing the full set of all sorted data.

Data in {{ ydb-short-name }} tables is always indexed by the primary key. This means that retrieving any record from a table with given values of the fields that make up the primary key will always take a minimum fixed time, regardless of the total number of records in the table. Also, having an index on the primary key allows you to retrieve any sequential range of records in ascending or descending order of the primary key. The execution time of such an operation will depend only on the number of records retrieved, but not on the total number of records in the table.

To use similar capabilities for any fields or combinations of fields in a table, additional indexes called **secondary indexes** can be built on them.

In transactional systems, using indexes helps reduce or eliminate performance degradation and increased query execution costs as the volume of stored data grows.

This article describes the basic operations for working with secondary indexes and provides links to detailed materials for each operation. Information about different types of secondary indexes and their features is in the Secondary indexes article in the "Concepts" section.

## Creating secondary indexes {#create}

A secondary index is a schema object and can be defined when creating a table using the [YQL `CREATE TABLE` command](../yql/reference/syntax/create_table/index.md), or added to it later using the [YQL `ALTER TABLE` command](../yql/reference/syntax/alter_table/index.md).

The [create index `table index add` command](../reference/ydb-cli/commands/secondary_index.md#add) is supported in the {{ ydb-short-name }} CLI.

Since an index contains its own data derived from the table data, when creating an index on an existing table with data, an initial index build operation will be performed, which may take a long time. This operation runs in the background, does not block work with the table, but until the build is complete, the new index cannot be used.

An index can only be used in the order of the fields included in it. If an index has two fields `a` and `b`, then such an index can be effectively used for queries of the form:

* `WHERE a = $var1 AND b = $var2`
* `WHERE a = $var1`
* `WHERE a > $var1`, as well as other comparison operators
* `WHERE a = $var1 AND b > $var2`, as well as any other comparison operators, but the first field must be checked for equality.

However, such an index cannot be used for the following queries:

* `WHERE b = $var1`
* `WHERE a > $var1 AND b > $var2`, more precisely, this record will be equivalent to `WHERE a > $var1` from the index usage perspective
* `WHERE b > $var1`.

Given the above feature, it is useless to try to index all possible column combinations in a table in advance in the hope of fast execution of any queries. An index is always a trade-off between search and write speed, as well as the storage space occupied by data. Indexes are created for specific queries and search conditions that will be made by the application in the database.

## Using secondary indexes in data retrieval {#use}

To access a table by a secondary index, its name must be explicitly specified in the `VIEW` section after the table name, as described in the article about the [`SELECT` command](../yql/reference/syntax/select#secondary_index) in YQL. For example, to retrieve from the Orders table (`orders`) a sample of orders for a customer with a given ID (`id_customer`), the query will look as follows:


```yql
DECLARE $customer_id AS Uint64;
SELECT *
FROM   orders VIEW idx_customer AS o
WHERE  o.id_customer = $customer_id
```


, where `idx_customer` is the name of the secondary index on the `orders` table, with the first field being `id_customer`.

Without specifying the `VIEW` section, the `orders` table will be fully scanned to execute such a query.

In transactional applications, such informational queries are executed using paginated data output, which prevents the cost and execution time from growing as the number of records matching the filter conditions increases. The approach to writing [paged queries](../dev/paging.md) described using the primary key example is also applicable to columns included in a secondary index.

An experimental feature for automatic selection of a secondary index to use in a query is also implemented. The selection algorithm is currently rule-based and uses only the query text to automatically select a secondary index.

### Automatic index usage in queries

{% note warning %}

This mechanism is experimental and is currently disabled by default. It can be enabled using the [`index_auto_choose_mode` setting in `table_service_config`](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/table_service_config.proto#L268). The setting will also affect the behavior of the query service.

{% endnote %}

Explicitly specifying the `VIEW` section takes precedence over the optimizer's decision to use secondary indexes. That is, the query


```yql
SELECT * FROM `Table` VIEW Index
```


will be guaranteed to perform a read using the `Index` index.

To explicitly specify reading using the primary key, use the following construct:


```yql
SELECT * FROM `Table` VIEW PRIMARY KEY
```


#### Secondary index selection criteria

The selection of the index used for reading occurs during query optimization when determining the ranges of rows to be read (predicate pushdown). Indexes, like the main table, consist of a set of rows ordered by a set of key columns.

The choice between reading using an index and reading using the primary key is based on the following metrics:

1. Need for additional reads from the main table. If the index contains all the columns required for the query, no additional reads are needed.
2. Length of the point prefix of the predicate for the key of the corresponding table. That is, the predicate restricts a set of columns that are the first components of the key with point conditions: `=`, `IN`, `IS NULL`. Here, priority is given to indexes for which all indexed columns are fixed, or to the main table if the primary key is entirely point-based.
3. Number of columns used in the read range boundaries. In the following query to the Table table with primary key (Key1, Key2, Key3)


```yql
SELECT * FROM `Table` WHERE (Key1, Key2, Key3) < ($param1, $param2, $param3) AND (Key1, Key2) > ($param4, $param5)
```


the read will be performed in the range `(($param4, $param5), ($param1, $param2, $param3))` and thus the number of columns used will be 3. Similarly, preference is given to indexes for which all indexed columns are used.

Read methods are ranked among themselves according to criterion 2, with criterion 3 used as a tiebreaker, and criterion 1 is additionally considered.

#### Automatic index selection examples


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


`SELECT * FROM Table WHERE SubKey1 = $p1 and SubKey2 > $p2` — `Index12` will be used. Range expression — `(($p1; $p2), ($p1)]`. Point prefix length for `Index12` — 1, for other indexes — 0.

`SELECT * FROM Table WHERE Key = $p1 and SubKey1 = $p2 And SubKey2 = $p2` — the index will not be used. When choosing a scan of the main table, all 3 columns `[Key, Fk1, Fk2]` are used, point prefix length 3.`

`SELECT * FROM Table WHERE Key = $p1 and SubKey2 = $p2` — secondary indexes will not be used. When choosing any secondary index, 1 column is used, the point prefix is also no more than 1 for any index selection option.

`SELECT * FROM Table WHERE Key >= $p1 and SubKey1 = $p2 And SubKey2 = $p3` — Index12 should be selected, because when it is selected, in the resulting range `[[Fk1; Fk2; Key], [Fk1; Fk2])`, the point prefix length will be 2, and 3 columns will be used.

`SELECT * FROM Table WHERE Key = 2 and SubKey2 = 3` — secondary indexes should not be used. In the case of reading by `PK` and when using any of the secondary indexes, the point prefix consists of no more than one column. Also, no more than one column is used.

`SELECT * FROM Table WHERE SubKey1 > 2` — `Index12` should be selected. Only when using `Index12` will there be a non-trivial range for reading.

`SELECT * FROM Table WHERE SubKey2 = 2` — any of `Index21` and `Index212` may be selected. When using the aforementioned indexes, the point prefix length will be 1. The number of columns used is also maximized when selecting `Index21` and `Index212`.

`SELECT Value2 FROM Table WHERE SubKey2 = 2` — Index212 should be selected. When using Index21 and Index212, the point prefix length will be 1, but when using Index212, no read from the main table is needed.

`SELECT * FROM Table WHERE SubKey2 > 2` — `Index21` or `Index212` will be used, since the read range is nontrivial only when they are used.

`SELECT * FROM Table WHERE SubKey1 = 2` — `Index12` will be used, since when it is used the point prefix length will be 1, and in other cases 0.

## Checking query cost {#cost}

Any query in a transactional application must be checked in terms of how many I/O operations it performed in the database and how much CPU was spent on its execution. It is also necessary to ensure that these numbers do not grow indefinitely as the database volume increases. In {{ ydb-short-name }}, after each query execution, statistics containing the information necessary for analysis are returned.

When using the {{ ydb-short-name }} CLI, the output of statistics after executing the `yql` command is enabled by the `--stats` option. All {{ ydb-short-name }} SDKs also contain structures that include statistics after query execution. When executing queries in the UI, next to the results tab there is also a statistics tab.

## Updating data using a secondary index {#update}

YQL commands for modifying records ( [`UPDATE`](../yql/reference/syntax/update.md), [`UPSERT`](../yql/reference/syntax/upsert_into.md), [`REPLACE`](../yql/reference/syntax/replace_into.md)) do not allow specifying the use of a secondary index for data search, so attempting to execute `UPDATE ... WHERE indexed_field = $value` will result in a full table scan. To avoid this, you can first execute `SELECT` using the index to obtain the primary key value, and then execute `UPDATE` using the primary key. You can also use the `UPDATE ON` statement.

To update data in the `table1` table, run the query:


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

To delete all data about series with zero views in the `series` table, run the query:


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

An existing secondary index can be replaced atomically. This can be useful, for example, for replacing an index with a covering one. For running applications, this operation is transparent — at the moment of index replacement, invalidation of compiled queries will occur.

To atomically replace an existing index, use the {{ ydb-short-name }} CLI command [{{ ydb-cli }} table index rename](../reference/ydb-cli/commands/secondary_index.md#rename) with the `--replace` parameter.

## Write performance for tables with secondary indexes {#write_performance}

Secondary indexes require additional data structures. Maintaining these structures leads to increased cost of data modification operations in tables.

With synchronous index updates, a transaction is committed only after writing all necessary data, both in the table and in the synchronous indexes. This leads to both increased execution time and the need to use [distributed transactions](../concepts/transactions#distributed-tx) even when adding or modifying records in a single partition.

Asynchronously updated indexes retain the ability to use single-shard transactions, but only guarantee eventual consistency, and still create load on the database.

# Сolumn-oriented table

{% note warning %}

Column-oriented {{ ydb-short-name }} tables are in the Preview mode.

{% endnote %}

A column-oriented table in {{ ydb-short-name }} is a relational table containing a set of related data and made up of rows and columns. Unlike regular [row-oriented {{ ydb-short-name }} tables](#table) designed for [OLTP loads](https://ru.wikipedia.org/wiki/OLTP), column-oriented tables are optimized for data analytics and [OLAP loads](https://ru.wikipedia.org/wiki/OLAP).

The current primary use case for column-oriented tables is writing data with the increasing primary key, for example, event time, analyzing this data, and deleting expired data based on TTL. The optimal method of inserting data to column-oriented tables is batch writing in blocks of several megabytes.

The data batches are inserted atomically: the data will be written either to all partitions or to none of them. Read operations analyze only the data fully written to your column-oriented tables.

In most cases, working with column-oriented {{ ydb-short-name }} tables is similar to row-oriented tables. However, there are the following distinctions:

* You can only use NOT NULL columns as your key columns.
* Data is not partitioned by the primary key but by the hash from the [partitioning columns](#olap-tables-partitioning).
* A [limited set](#olap-data-types) of data types is supported.

What's currently not supported:

* Reading data from replicas
* Secondary indexes
* Bloom filters
* Change Data Capture
* Renaming tables
* Custom attributes in tables
* Updating data column lists in column-oriented tables
* Adding data to column-oriented tables by the SQL `INSERT` operator
* Deleting data from column-oriented tables using the SQL `DELETE` operator The data is actually deleted on TTL expiry.

## Supported data types {#olap-data-types}

| Data type | Can be used in<br/>column-oriented tables | Can be used<br/>as primary key |
---|---|---
| `Bool` | ☓ | ☓ |
| `Date` | ✓ | ✓ |
| `Datetime` | ✓ | ✓ |
| `Decimal` | ☓ | ☓ |
| `Double` | ✓ | ☓ |
| `Float` | ✓ | ☓ |
| `Int16` | ✓ | ☓ |
| `Int32` | ✓ | ✓ |
| `Int64` | ✓ | ✓ |
| `Int8` | ✓ | ☓ |
| `Interval` | ☓ | ☓ |
| `JsonDocument` | ✓ | ☓ |
| `Json` | ✓ | ☓ |
| `String` | ✓ | ✓ |
| `Timestamp` | ✓ | ✓ |
| `Uint16` | ✓ | ✓ |
| `Uint32` | ✓ | ✓ |
| `Uint64` | ✓ | ✓ |
| `Uint8` | ✓ | ✓ |
| `Utf8` | ✓ | ✓ |
| `Uuid` | ☓ | ☓ |
| `Yson` | ✓ | ☓ |

Learn more in [{#T}](../yql/reference/types/index.md).

## Partitioning {#olap-tables-partitioning}

Unlike row-oriented {{ ydb-short-name }} tables, you cannot partition column-oriented tables by primary keys but only by specially designated partitioning keys. Partitioning keys constitute a subset of the table's primary keys.

Unlike data partitioning in row-oriented {{ ydb-short-name }} tables, key values are not used to partition data in column-oriented tables. Hash values from keys are used instead. This way, you can uniformly distribute data across all your existing partitions. This kind of partitioning enables you to avoid hotspots at data insert, streamlining analytical queries that process (that is, read) large data amounts.

How you select partitioning keys substantially affects the performance of your column-oriented tables. Learn more in [{#T}](../best_practices/pk-olap-scalability.md).

To manage data partitioning, use the `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` additional parameter. The system ignores other partitioning parameters for column-oriented tables.

`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` sets the minimum physical number of partitions used to store data.

* Type: `Uint64`.
* The default value is `1`.

Because it ignores all the other partitioning parameters, the system uses the same value as the upper partition limit.

## See also {#see-also}

* [{#T}](../yql/reference/syntax/create_table.md#olap-tables)

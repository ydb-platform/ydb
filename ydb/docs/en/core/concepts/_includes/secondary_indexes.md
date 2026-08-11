# Secondary Indexes

In {{ ydb-short-name }}, an index on the primary key is created automatically, so queries with a condition on the primary key always run efficiently, affecting only the required rows. A query with a condition on one or more non-key columns typically results in a full table scan. To make such queries efficient, you need to use _secondary indexes_ — global structures with a separate index table.

Separately, there are [local indexes](../glossary.md#local-index): auxiliary structures that are stored together with the table data and are used when reading on the storage side, without materializing a separate index table (see the [Local indexes](#local-skip-index) section below).

The current version of {{ ydb-short-name }} implements _synchronous_ and _asynchronous_ global secondary indexes. Each index is a hidden table that is updated:

* For synchronous indexes, transactionally when the main table is modified.
* For asynchronous indexes, in the background, receiving the necessary changes from the main table.

When a user sends an SQL query to insert, modify, or delete data, the database transparently generates commands to modify the index table. A table can have multiple secondary indexes. An index can include multiple columns, and the order of columns in the index matters. A single column can be included in multiple indexes. In addition to the specified columns, the index always implicitly stores the values of the table's primary key columns, so that you can move from a found record in the index to a record in the table.

## Synchronous Secondary Index {#sync}

A synchronous index is updated simultaneously with the table that it indexes. This index ensures [strict consistency](https://en.wikipedia.org/wiki/Consistency_model) through [distributed transactions](../transactions.md#distributed-tx). While reads and blind writes to a table with no index can be performed without a planning stage, significantly reducing delays, such optimization is impossible when writing data to a table with a synchronous index.

## Asynchronous Secondary Index {#async}

Unlike a synchronous index, an asynchronous index doesn't use distributed transactions. Instead, it receives changes from an indexed table in the background. Write transactions to a table using this index are performed with no planning overheads due to reduced guarantees: an asynchronous index provides [eventual consistency](https://en.wikipedia.org/wiki/Eventual_consistency), but no strict consistency. You can only use asynchronous indexes in read transactions in [Stale Read Only](../transactions.md#modes) mode.

## Covering Secondary Index {#covering}

You can copy the contents of columns into the index (covering index), which eliminates the need to read from the main table in index read operations, significantly reducing latency. At the same time, such denormalization leads to increased disk space consumption and may slow down insert and update operations due to the need for additional data copying.

## Vector Index

[Vector Index](../../dev/vector-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes, which optimize equality or range searches, vector indexes allow [vector search](../query_execution/vector_search.md) based on distance or similarity functions.

## Fulltext Index

[Fulltext index](../../dev/fulltext-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes, which optimize equality or range searches, fulltext indexes allow scalable text search by words and phrases (and, with n-grams, by substrings). See also: [Fulltext search](../query_execution/fulltext_search.md).

## Local indexes {#local-skip-index}

[Local indexes](../query_execution/local_indexes.md) are auxiliary structures stored together with the table data and used when reading on the storage side. They do not materialize a separate index table. Currently, [Bloom indexes](../../dev/bloom-skip-indexes.md) and [min_max index](../../dev/min_max-skip-index.md) are implemented.

## Creating a Secondary Index Online {#index-add}

In {{ ydb-short-name }}, you can create a secondary index and delete an existing secondary index without stopping service. You can create only one index at a time for a single table.

Online index creation consists of the following steps:

1. Taking a snapshot of the table with data, creating the index table marked as available for writing.

   After this step, write transactions become distributed, and writes occur to both the main table and the index. The index is not yet available to the user.
2. Reading the snapshot of the main table and writing to the index.

   A 'write to the past' is implemented: situations are resolved where data updates in step 1 change data written in step 2.
3. Publishing the result, deleting the snapshot.

   The index is ready for use.

Possible impact on user transactions:

* There may be an increase in delays because transactions are now distributed (when creating a synchronous index).
* There may be an enhanced background of `OVERLOADED` errors because index table automatic shard splitting is actively running during data writes.

{% note info %}

The data write rate is chosen to minimize the impact of the write process on user transactions. To control the rate, configure limits for the corresponding queue in the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

Index creation is an asynchronous operation. If a client-server connection breaks after the operation starts, index building will continue. You can manage the asynchronous operation via the {{ ydb-short-name }} CLI.

## Creating and Deleting Secondary Indexes {#ddl}

A secondary index can be:

- Created when creating a table with the YQL [`CREATE TABLE`](../../yql/reference/syntax/create_table/index.md) statement.
- Added to an existing table with the YQL [`ALTER TABLE`](../../yql/reference/syntax/alter_table/index.md) statement or the YDB CLI [`table index add`](../../reference/ydb-cli/commands/secondary_index.md#add) command.
- Deleted from an existing table with the YQL [`ALTER TABLE`](../../yql/reference/syntax/alter_table/index.md) statement or the YDB CLI [`table index drop`](../../reference/ydb-cli/commands/secondary_index.md#drop) command.
- Deleted together with the table using the YQL [`DROP TABLE`](../../yql/reference/syntax/drop_table.md) statement or the YDB CLI `table drop` command.

## Using Secondary Indexes {#use}

For detailed information on using secondary indexes in applications, refer to the [relevant article](../../dev/secondary-indexes.md) in the documentation section for developers.

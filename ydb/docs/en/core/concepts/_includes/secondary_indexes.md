# Secondary indexes

In {{ ydb-short-name }}, an index is automatically created on the primary key, so queries with a condition on the primary key are always efficient, affecting only the required rows. A query with a condition on one or more non-key columns typically results in a full table scan. To make such queries efficient, you should use _secondary indexes_ — global structures with a separate index table.

Separately, there are [local indexes](../glossary.md#local-index): auxiliary structures that are stored together with the table data and are used when reading on the storage side, without materializing a separate index table (see the [Local indexes](#bloom-skip-index) section below).

In the current version of {{ ydb-short-name }}, _synchronous_ and _asynchronous_ global secondary indexes are implemented. Each index is a hidden table that is updated:

* for synchronous indexes — transactionally when the main table is modified;
* for asynchronous indexes — in the background, receiving the necessary changes from the main table.

When a user sends an SQL query to insert, modify, or delete data, the database transparently generates commands to modify the index table. A table can have multiple secondary indexes. An index can include multiple columns, and the order of columns in the index is important. A single column can be included in multiple indexes. In addition to the specified columns, the index always implicitly stores the values of the table's primary key columns, so that from a found index entry, you can navigate to the table entry.

## Synchronous secondary index {#sync}

A synchronous index is updated simultaneously with the table it indexes. Such an index provides [strong data consistency](https://en.wikipedia.org/wiki/Consistency_model) and uses the [distributed transaction](../transactions.md#distributed-tx) mechanism for this. Thus, while read and blind write operations on a table without an index can be performed without a planning stage, thereby significantly reducing latency, such optimization is not possible for writes to a table with a synchronous index.

## Asynchronous secondary index {#async}

An asynchronous index, unlike a synchronous one, does not use the distributed transaction mechanism, but receives changes from the indexed table in the background. Write transactions to a table with such an index are performed without additional planning overhead, at the cost of reduced guarantees: an asynchronous index provides [eventual data consistency](https://en.wikipedia.org/wiki/Eventual_consistency), but not strong consistency. Using an asynchronous index in read transactions is only possible in [Stale Read Only](../transactions.md#modes) mode.

## Covering secondary index {#covering}

It is possible to copy the contents of columns into the index (covering index), thus eliminating the need to read from the main table in index read operations, which significantly reduces latency. At the same time, such denormalization leads to increased disk space consumption and possible slowdown of insert and update operations due to the need for additional data copying.

## Unique secondary index {#unique}

This type of index implements the semantics of a unique value in a column or set of columns, and, like other indexes, allows efficient point reads on the set of indexed columns. {{ ydb-short-name }} uses it to perform additional checks to ensure that each unique value of the indexed columns appears in the table no more than once. If a modifying query violates this constraint, it is canceled with the status `PRECONDITION_FAILED`. Therefore, user code must be prepared to handle this status.

A unique secondary index is a synchronous index, so from a transactional perspective, its update process is the same as that of the [synchronous secondary index](#sync) described above.

## Vector index {#vector}

[Vector index](../../dev/vector-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes that optimize equality or range search, vector indexes enable [vector search](../query_execution/vector_search.md) based on distance or similarity functions.

## Full-text index {#fulltext}

[Full-text index](../../dev/fulltext-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes, which optimize equality or range search, full-text indexes allow scalable text search by words and phrases (and when using [N-grams](https://en.wikipedia.org/wiki/N-gram), also by substrings). See also: [Full-text search](../query_execution/fulltext_search.md).

## JSON index {#json}

[JSON index](../../dev/json-indexes.md) is a special kind of secondary index, like the full-text index — both are built on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but use different tokenizers.

JSON indexes allow you to speed up predicates with the [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) functions on the content of a column of type `Json` or `JsonDocument`. The index is built by splitting JSON documents into path tokens and pairs of the form «path + value», which allows you to find matching rows by [JsonPath](../../yql/reference/builtins/json.md#jsonpath) paths without a full table scan. See also: [Search by JSON](../query_execution/json_search.md).

## Local indexes {#bloom-skip-index}

[Local indexes](../query_execution/local_indexes.md) — auxiliary structures stored together with table data and used for reads on the storage side. They do not materialize a separate index table. Currently, [Bloom indexes](../../dev/bloom-skip-indexes.md) are implemented, and other types are planned for the future.

## Online creation of a secondary index {#index-add}

In {{ ydb-short-name }}, you can create a secondary index and delete an existing secondary index without stopping service. Only one index can be created per table at a time.

The online index creation operation consists of the following steps:

1. Taking a snapshot of the data table, creating an index table marked as available for writing.

   After this step, write transactions become distributed, and records are written to the main table and the index. The index is not yet available to the user.
2. Reading a snapshot of the base table and writing to the index.

   A "write to the past" is implemented: situations are allowed where data updates in step 1 change data written in step 2.
3. Publishing the result, deleting the snapshot.

   The index is ready to use.

Possible impact on user transactions:

* Increased delays may be observed because transactions become distributed (when creating a synchronous index).
* A higher rate of `OVERLOADED` errors may occur because the automatic splitting of index table shards is actively working during data writes.

{% note info %}

The data write speed is chosen to minimize the impact of the write process on user transactions. To control the speed, configure limits for the corresponding queue of the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

Creating an index is an asynchronous operation. If the client-server connection is broken after the operation is started, index building will continue. You can manage the asynchronous operation via the {{ ydb-short-name }} CLI.

## Creating and deleting secondary indexes {#ddl}

A secondary index can be:

- Created when creating a table using the YQL [CREATE TABLE](../../yql/reference/syntax/create_table/index.md) command.
- Added to an existing table by the YQL [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) command or by the {{ ydb-short-name }} CLI [table index add](../../reference/ydb-cli/commands/secondary_index.md#add) command.
- Removed from an existing table by the YQL command [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) or by the {{ ydb-short-name }} CLI command [table index drop](../../reference/ydb-cli/commands/secondary_index.md#drop).
- Deleted together with the table by the YQL command [DROP TABLE](../../yql/reference/syntax/drop_table.md) or by the {{ ydb-short-name }} CLI command `table drop`.

## Using secondary indexes {#use}

Detailed information about using secondary indexes in applications is available in [the article about them](../../dev/secondary-indexes.md) in the documentation section for developers.

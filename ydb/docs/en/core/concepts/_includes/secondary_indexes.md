# Secondary indexes

In {{ ydb-short-name }} an index on the primary key is created automatically, so queries with a condition on the primary key are always executed efficiently, touching only the required rows. A query with a condition on one or more non‑key columns typically results in a full table scan. To make such queries efficient, you need to use *secondary indexes* — global structures with a separate index table.

Separate from that, there are [local indexes](../glossary.md#local-index): auxiliary structures that are stored together with the table data and are applied during storage‑side reads, without materializing a separate index table (see the [Local indexes](#bloom-skip-index) section below).

In the current version, {{ ydb-short-name }} implements *synchronous* and *asynchronous* global secondary indexes. Each index is a hidden table that is updated:

* for synchronous indexes — transactionally when the primary table changes.
* for asynchronous indexes — in the background, receiving the necessary changes from the primary table.

When a user sends an SQL query to insert, update, or delete data, the database transparently generates commands to modify the index table. A table can have multiple secondary indexes. An index can include multiple columns, and the order of columns in the index matters. A column can be part of several indexes. In addition to the specified columns, an index always implicitly stores the values of the table’s primary key columns, so that from a found index entry you can navigate to the corresponding table row.

## Synchronous secondary index {#sync}

A synchronous index is updated together with the table it indexes. Such an index provides [strict data consistency](https://en.wikipedia.org/wiki/Consistency_model) and uses the [distributed transactions](../transactions.md#distributed-tx) mechanism for this. Thus, while read operations and blind writes to a table without an index can be performed without a planning stage, significantly reducing latency, this optimization is not possible for writes to a table with a synchronous index.

## Asynchronous secondary index {#async}

An asynchronous index, unlike a synchronous one, does not use the distributed transactions mechanism, but instead receives changes from the indexed table in the background. Write transactions to a table with such an index are executed without additional planning overhead, at the cost of reduced guarantees: an asynchronous index provides [eventual data consistency](https://en.wikipedia.org/wiki/Eventual_consistency) but not strict consistency. Using an asynchronous index in read transactions is possible only in the [Stale Read Only](../transactions.md#modes) mode.

## Covering secondary index {#covering}

It is possible to copy the contents of columns into the index (covering index), which eliminates the need to read from the primary table during index read operations, significantly reducing latency. At the same time, this denormalization increases disk space consumption and can slow down insert and update operations due to the need for additional data copying.

## Unique secondary index {#unique}

This type of index implements the semantics of a unique value in a column or set of columns and, like other indexes, allows efficient point reads on the set of indexed columns. {{ ydb-short-name }} uses it to perform additional checks to ensure that each unique value of the indexed columns appears in the table at most once. If a modifying query violates this constraint, it is aborted with status `PRECONDITION_FAILED`. Consequently, client code must be prepared to handle this status.

A unique secondary index is a synchronous index, so from a transactional perspective its update process is the same as that of the [synchronous secondary index](#sync) described above.

## Vector index {#vector}

[Vector index](../../dev/vector-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes that optimize equality or range searches, vector indexes enable you to perform [vector search](../query_execution/vector_search.md) based on distance or similarity functions.

## Full-text index {#fulltext}

[Full-text index](../../dev/fulltext-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes that optimize equality or range searches, full-text indexes allow scalable text search by words and phrases (and, when using [N-grams](https://en.wikipedia.org/wiki/N-gram) — also by substrings). See also: [Full-text search](../query_execution/fulltext_search.md).

## JSON index {#json}

[JSON index](../../dev/json-indexes.md) is a special type of secondary index, like the full-text one — both are built on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but use different tokenizers.

JSON indexes allow speeding up predicates with the [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) functions on the contents of a column of type `Json` or `JsonDocument`. The index is built by splitting JSON documents into path tokens and pairs of the form “path + value”, which enables finding matching rows by [JsonPath](../../yql/reference/builtins/json.md#jsonpath) paths without a full table scan. See also: [JSON search](../query_execution/json_search.md).

## Local indexes {#bloom-skip-index}

[Local indexes](../query_execution/local_indexes.md) — auxiliary structures stored together with the table data and used during reads on the storage side. They do not materialize a separate index table. Currently, [Bloom indexes](../../dev/bloom-skip-indexes.md) are implemented, and other types are planned for the future.

## Online creation of a secondary index {#index-add}

In {{ ydb-short-name }} you can create a secondary index and also delete an existing secondary index without service interruption. For a single table you can create only one index at a time.

The online index creation operation consists of the following steps:

1. Taking a snapshot of the table with data, creating an index table with a write-availability marker.

   After this step, write transactions become distributed, and records are written to the primary table and the index. The index is not yet available to the user.
2. Reading a snapshot of the primary table and writing to the index.

   Implements “write-to-the-past”: it allows situations where data updates at step 1 change data written at step 2.
3. Publishing the result, deleting the snapshot.

   The index is ready for use.

Potential impact on user transactions:

* Increased latency may be observed because transactions become distributed when creating a synchronous index.
* You may experience an increased error rate `OVERLOADED` because automatic shard splitting of the index table is actively running while data is being written.

{% note info %}

The data write speed is selected to minimize the impact of the write process on user transactions. To control the speed, configure limits for the appropriate [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config) queue.

{% endnote %}

Creating an index is an asynchronous operation. If a client-server connectivity break occurs after the operation is started, index building will continue. You can manage the asynchronous operation via the {{ ydb-short-name }} CLI.

## Creating and deleting secondary indexes {#ddl}

A secondary index can be:

- Created when a table is created with the YQL [CREATE TABLE](../../yql/reference/syntax/create_table/index.md) command.
- Added to an existing table by the YQL command [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) or by the {{ ydb-short-name }} CLI command [table index add](../../reference/ydb-cli/commands/secondary_index.md#add)
- Deleted from an existing table using the YQL command [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) or the {{ ydb-short-name }} CLI command [table index drop](../../reference/ydb-cli/commands/secondary_index.md#drop).
- It is removed together with the table by the YQL command [DROP TABLE](../../yql/reference/syntax/drop_table.md) or by the {{ ydb-short-name }} CLI `table drop` command.

## Using secondary indexes {#use}

Detailed information about using secondary indexes in applications can be found in the [article about them](../../dev/secondary-indexes.md) in the developer documentation section.

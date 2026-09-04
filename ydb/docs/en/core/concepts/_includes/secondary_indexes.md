# Secondary indexes

In {{ ydb-short-name }}, an index on the primary key is created automatically, so queries with a condition on the primary key always run efficiently, affecting only the required rows. A query with a condition on one or more non-key columns typically results in a full table scan. To make such queries efficient, you need to use _secondary indexes_ — global structures with a separate index table.

Separately, there are [local indexes](../glossary.md#local-index): auxiliary structures that are stored together with the table data and are used when reading on the storage side, without materializing a separate index table (see the [Local indexes](#local-skip-index) section below).

The current version of {{ ydb-short-name }} implements _synchronous_ and _asynchronous_ global secondary indexes. Each index is a hidden table that is updated:

* For synchronous indexes, transactionally when the main table is modified.
* For asynchronous indexes, in the background, receiving the necessary changes from the main table.

When a user sends an SQL query to insert, modify, or delete data, the database transparently generates commands to modify the index table. A table can have multiple secondary indexes. An index can include multiple columns, and the order of columns in the index matters. A single column can be included in multiple indexes. In addition to the specified columns, the index always implicitly stores the values of the table's primary key columns, so that you can move from a found record in the index to a record in the table.

## Synchronous secondary index {#sync}

A synchronous index is updated simultaneously with the table it indexes. Such an index provides [strong data consistency](https://en.wikipedia.org/wiki/Consistency_model) and uses the [distributed transactions](../transactions.md#distributed-tx) mechanism for this. Thus, while read and blind write operations on a table without an index can be performed without a planning stage, thereby significantly reducing latency, such optimization is not possible for writes to a table with a synchronous index.

## Asynchronous secondary index {#async}

An asynchronous index, unlike a synchronous one, does not use the distributed transaction mechanism, but receives changes from the indexed table in the background. Write transactions to a table with such an index are performed without additional planning overhead, at the cost of reduced guarantees: an asynchronous index provides [eventual data consistency](https://en.wikipedia.org/wiki/Eventual_consistency), but not strong consistency. Using an asynchronous index in read transactions is only possible in [Stale Read Only](../transactions.md#modes) mode.

## Covering secondary index {#covering}

You can copy the contents of columns into the index (covering index), which eliminates the need to read from the main table in index read operations, significantly reducing latency. At the same time, such denormalization leads to increased disk space consumption and may slow down insert and update operations due to the need for additional data copying.

## Unique secondary index {#unique}

This type of index implements the semantics of a unique value in a column or set of columns, and, like other indexes, allows efficient point reads on the set of indexed columns. {{ ydb-short-name }} uses it to perform additional checks to ensure that each unique value of the indexed columns appears in the table no more than once. If a modifying query violates this constraint, it is aborted with the status `PRECONDITION_FAILED`. Therefore, user code must be prepared to handle this status.

A unique secondary index is a synchronous index, so from a transactional perspective, its update process is the same as that of the [synchronous secondary index](#sync) described above.

## Vector index {#vector}

[Vector index](../../dev/vector-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes, which optimize equality or range search, vector indexes allow performing [vector search](../query_execution/vector_search.md) based on distance or similarity functions.

## Full-text index {#fulltext}

[Full-text index](../../dev/fulltext-indexes.md) is a special type of secondary index.

Unlike traditional secondary indexes, which optimize search by equality or range, full-text indexes allow scalable text search for words and phrases (and when using [N-grams](https://en.wikipedia.org/wiki/N-gram), also for substrings). See also: [Full-text search](../query_execution/fulltext_search.md).

## JSON-index {#json}

[JSON index](../../dev/json-indexes.md) is a special type of secondary index, like full-text index — both are built on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but use different tokenizers.

JSON indexes allow you to speed up predicates with the [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) functions on the content of a column of type `Json` or `JsonDocument`. The index is built by splitting JSON documents into path tokens and pairs of the form "path + value", which allows you to find matching rows by [JsonPath](../../yql/reference/builtins/json.md#jsonpath) paths without a full table scan. See also: [JSON search](../query_execution/json_search.md).

## Local indexes {#local-skip-index}

[Local indexes](../query_execution/local_indexes.md) are auxiliary structures stored together with the table data and used when reading on the storage side. They do not materialize a separate index table. Currently, [Bloom indexes](../../dev/bloom-skip-indexes.md) and [min_max index](../../dev/min_max-skip-index.md) are implemented.

## Online creation of a secondary index {#index-add}

In {{ ydb-short-name }}, you can create a secondary index and delete an existing secondary index without stopping service. You can create only one index at a time for a single table.

The online index creation operation consists of the following steps:

1. Taking a snapshot of the table with data, creating the index table marked as available for writing.

   After this step, write transactions become distributed, and writes occur to both the main table and the index. The index is not yet available to the user.
2. Reading the snapshot of the main table and writing to the index.

   A 'write to the past' is implemented: situations are resolved where data updates in step 1 change data written in step 2.
3. Publishing the result, deleting the snapshot.

   The index is ready for use.

Possible impact on user transactions:

* Increased latency may be observed because transactions become distributed (when creating a synchronous index).
* An increased background of `OVERLOADED` errors is possible because automatic partitioning of index table shards is actively working during data writes.

{% note info %}

The data write speed is chosen to minimize the impact of the write process on user transactions. To control the speed, configure limits for the corresponding queue of the [resource broker](../../reference/configuration/resource_broker_config.md#resource-broker-config).

{% endnote %}

Index creation is an asynchronous operation. If a client-server connection breaks after the operation starts, index building will continue. You can manage the asynchronous operation via the {{ ydb-short-name }} CLI.

## Creating and deleting secondary indexes {#ddl}

A secondary index can be:

- Created when creating a table using the YQL [CREATE TABLE](../../yql/reference/syntax/create_table/index.md) command.
- Added to an existing table by the YQL [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) command or by the {{ ydb-short-name }} CLI [table index add](../../reference/ydb-cli/commands/secondary_index.md#add) command.
- Deleted from an existing table by the YQL command [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md) or by the {{ ydb-short-name }} CLI command [table index drop](../../reference/ydb-cli/commands/secondary_index.md#drop).
- Deleted together with the table by the YQL command [DROP TABLE](../../yql/reference/syntax/drop_table.md) or by the {{ ydb-short-name }} CLI command `table drop`.

## Using secondary indexes {#use}

Detailed information on using secondary indexes in applications is available in [the article about them](../../dev/secondary-indexes.md) in the developer documentation section.

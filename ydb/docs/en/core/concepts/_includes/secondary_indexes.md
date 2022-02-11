# Secondary indexes

{{ ydb-short-name }} automatically creates a primary key index, which is why selection by primary key is always efficient, affecting only the rows needed. Selections by criteria applied to one or more non-key columns typically result in a full table scan. To make these selections efficient, use _secondary indexes_.

The current version of {{ ydb-short-name }} implements _synchronous_ and _asynchronous_ global secondary indexes. Each index is a hidden table that is updated:

* For synchronous indexes: Transactionally when the main table changes.
* For asynchronous indexes: In the background while getting the necessary changes from the main table.

When a user sends an SQL query to insert, modify, or delete data, the database transparently generates commands to modify the index table. A table may have multiple secondary indexes. An index may include multiple columns, and the sequence of columns in an index matters. A single column may consist of multiple indexes and be part of a primary key and a secondary index at the same time.

## Synchronous secondary index {#sync}

A synchronous index is updated simultaneously with the table that it indexes. This index ensures [strict consistency](https://en.wikipedia.org/wiki/Consistency_model) through [distributed transactions](../transactions.md#distributed-tx). While reads and blind writes to a table with no index can be performed without a planning stage, significantly reducing delays, such optimization is impossible when writing data to a table with a synchronous index.

## Asynchronous secondary index {#async}

Unlike a synchronous index, an asynchronous index doesn't use distributed transactions. Instead, it receives changes from an indexed table in the background. Write transactions to a table using this index are performed with no planning overheads due to reduced guarantees: an asynchronous index provides [eventual consistency](https://en.wikipedia.org/wiki/Eventual_consistency), but no strict consistency. You can only use asynchronous indexes in read transactions in [Stale Read Only](transactions.md#modes) mode.

## Covering secondary index {#covering}

You can copy the contents of columns into a covering index. This eliminates the need to read data from the main table when performing reads by index and significantly reduces delays. At the same time, such denormalization leads to increased usage of disk space and may slow down inserts and updates due to the need for additional data copying.

## Creating a secondary index online {#index-add}

{{ ydb-short-name }} lets you create new and delete existing secondary indexes without stopping the service. For a single table, you can only create one index at a time.

Online index creation consists of the following steps:

1. Taking a snapshot of a data table and creating an index table marked that writes are available.

    After this step, write transactions are distributed, writing to the main table and the index, respectively. The index is not yet available to the user.

1. Reading the snapshot of the main table and writing data to the index.

    "Writes to the past" are implemented: situations where data updates in step 1 change the data written in step 2 are resolved.

1. Publishing the results and deleting the snapshot.

    The index is ready to use.

Possible effects on user transactions:

* There may be an increase in delays because transactions are now distributed (when creating a synchronous index).
* There may be an enhanced background of `OVERLOADED` errors because index table automatic shard splitting is actively running during data writes.

The rate of data writes is selected to minimize their impact on user transactions. To quickly complete the operation, we recommend running the online creation of a secondary index when the user load is minimum.

Creating an index is an asynchronous operation. If the client-server connection is interrupted after the operation has started, index building continues. You can manage asynchronous operations using the {{ ydb-short-name }} CLI.

## Examples of working with a secondary index {#example}

### Creating a secondary index {#add}

{% list tabs %}

- CLI

  Run the command:

  ```bash
  {{ ydb-cli }} \
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    table index add global[-sync|-async] \
    --index-name title_index \
    --columns title \
    series
  ```

  * `--endpoint`: DB endpoint.
  * `--database`: Full DB path.
  * `-sync|-async`: Index type.
  * `--index-name`: The name of the index being created.
  * `--columns`: A list of columns the index is created by.

  If necessary, you can duplicate data in a covering index by adding the `--cover` option that specifies a list of columns to copy data to this index from.

  The create index command looks like this:

  ```bash
  {{ ydb-cli }} \
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    table index add global[-sync|-async] \
    --index-name title_index \
    --columns title \
    --cover release_date \
    series
  ```

  Result:

  ```text
  ┌────────────────────────────────────────┬───────┬────────┐
  | id                                     | ready | status |
  ├────────────────────────────────────────┼───────┼────────┤
  | ydb://buildindex/7?id=1407375091598308 | false |        |
  └────────────────────────────────────────┴───────┴────────┘
  ```

  The build index operation is started, where `id` is the operation ID.

- YQL

  Run the query:

  ```sql
  ALTER TABLE `series` ADD INDEX `title_index` GLOBAL [SYNC|ASYNC] ON (`title`);
  ```

  If necessary, you can duplicate data in a covering index by using the `COVER` keyword:

  ```sql
  ALTER TABLE `series` ADD INDEX `title_index` GLOBAL [SYNC|ASYNC] ON (`title`) COVER(`release_date`);
  ```

  This starts building the index. Wait for the operation to complete.

  {% note warning %}

  You can't cancel index building with YQL. If necessary, use the {{ ydb-short-name }} CLI.

  {% endnote %}

{% endlist %}

{% note info %}

If you don't specify the index type, a synchronous index is created by default.

{% endnote %}

### Getting the status of a build secondary index operation {#get}

{% list tabs %}

- CLI

  Run the command:

  ```bash
  {{ ydb-cli }} \
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    operation get ydb://buildindex/7?id=1407375091598308
  ```
  * `--endpoint`: DB endpoint.
  * `--database`: Full DB path.

  Result:

  ```text
  ┌────────────────────────────────────────┬───────┬─────────┬───────┬──────────┬───────────────────────────────────────────────────────────────┬─────────────┐
  | id                                     | ready | status  | state | progress | table                                                         | index       |
  ├────────────────────────────────────────┼───────┼─────────┼───────┼──────────┼───────────────────────────────────────────────────────────────┼─────────────┤
  | ydb://buildindex/7?id=1407375091598308 | true  | SUCCESS | Done  | 100.00%  | /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj/series | title_index |
  └────────────────────────────────────────┴───────┴─────────┴───────┴──────────┴───────────────────────────────────────────────────────────────┴─────────────┘
  ```

{% endlist %}

### Canceling the build secondary index operation {#cancel}

{% list tabs %}

- CLI

  Run the command:

  ```bash
  {{ ydb-cli }} \
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    operation cancel ydb://buildindex/7?id=1407375091598308
  ```
  * `--endpoint`: DB endpoint.
  * `--database`: Full DB path.

{% endlist %}

### Deleting a completed or canceled build secondary index operation {#forget}

{% list tabs %}

- CLI

  Run the command:

  ```bash
  {{ ydb-cli }} \
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    operation forget ydb://buildindex/7?id=1407375091598308
  ```
  * `--endpoint`: DB endpoint.
  * `--database`: Full DB path.

{% endlist %}

### Deleting a secondary index {#drop}

{% list tabs %}

- CLI

  Run the command:

  ```bash
    --endpoint ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1g4ej5ju4rf5kelpk4b/etn01lrprvnlnhv8v5kj \
    table index drop \
    --index-name title_index \
    series
  ```
  * `--endpoint`: DB endpoint.
  * `--database`: Full DB path.
  * `--index-name`: The name of the index to delete.

- YQL

  Run the query:

  ```sql
  ALTER TABLE `series` DROP INDEX `title_index`;
  ```

{% endlist %}

#### What's next

For other examples of working with secondary indexes, see the [recommendations](../../best_practices/secondary_indexes.md).

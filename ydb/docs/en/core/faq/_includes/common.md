---
title: "Yandex Database. FAQ"
description: "What is Yandex Database? For what tasks is it worth using Yandex Database, and for which virtual machines with databases? What part of the management and maintenance of databases does Yandex Database take on? Answers to these and other questions in this article."
---
# General questions about {{ ydb-short-name }}

#### What is {{ ydb-short-name }}? {#what-is-ydb}

{{ ydb-short-name }} is a distributed fault-tolerant SQL DBMS. {{ ydb-short-name }}  provides high availability and scalability while simultaneously ensuring strict consistency and ACID transaction support. Queries are made using an SQL dialect (YQL).

{{ ydb-short-name }} is a fully managed database. DB instances are created through the {{ ydb-short-name }} database management service.

#### What features does {{ ydb-short-name }} provide? {#ydb-features}

{{ ydb-short-name }} provides high availability and data security through synchronous replication in three availability zones. {{ ydb-short-name }} also ensures even load distribution across available hardware resources. This means you don't need to order resources, {{ ydb-short-name }} automatically provisions and releases resources based on the user load.

#### What consistency model does {{ ydb-short-name }} use? {#ydb-consistency}

To read data, {{ ydb-short-name }} uses a model of strict data consistency.

#### How do I design a primary key? {#create-pk}

To design the primary key properly, follow the rules given below.

* Avoid situations where the main load falls on a single partition of a table. With even load distribution, it's easier to achieve high overall performance.

  This rule implies that you shouldn't use a monotonically increasing sequence, such as timestamp, as a table's primary key.

* The fewer table partitions used during query execution, the faster it's executed. For greater performance, follow the one query — one partition rule.

* Avoid situations where a small part of the DB is under much heavier load than the rest of the DB.

For more information, see [Schema design](../../best_practices/schema_design.md).

#### How do I evenly distribute load across table partitions? {#balance-shard-load}

You can use the following techniques to evenly distribute the load across table partitions and increase overall DB performance.

* To avoid using uniformly increasing primary key values, you can:
  * Change the order of its components.
  * use a hash of the key column values as the primary key.

* Reduce the number of partitions used in a single query.

For more information, see [Schema design](../../best_practices/schema_design.md#balance-shard-load).

#### Can I use NULL in a key column? {#null}

In {{ ydb-short-name }}, all columns, including key ones, may contain a `NULL` value, but we don't recommend using ` NULL` as values in key columns.

According to the SQL standard (ISO/IEC 9075), you can't compare `NULL` with other values. Therefore, the use of concise SQL statements with simple comparison operators may lead to skipping rows containing NULL during filtering, for example.

#### Is there an optimal size of a database row? {#string-size}

To achieve high performance, we don't recommend writing rows larger than 8 MB and key columns larger than 2 KB to the DB.

For more information about limits, see [Database limits](../../concepts/limits-ydb.md).

#### How are secondary indexes used in {{ ydb-short-name }}? {#secondary_indexes}

Secondary indexes in {{ ydb-short-name }} are global and can be non-unique.

Read more in the [documentation](../../concepts/secondary_indexes.md).

#### How is paginated output performed? {#paging}

To organize paginated output, we recommend selecting data sorted by primary key sequentially, limiting the number of rows with the `LIMIT` keyword. We do not recommend using the `OFFSET` keyword to solve this problem.

For more information, see [Paginated output](../../best_practices/paging.md).

#### How do I effectively upload large amounts of data to {{ ydb-short-name }}? {#batch_upload}

To speed up uploading large amounts of data, follow these recommendations:

* When creating a table, explicitly specify the required number of partitions or their boundaries. This will help you effectively use system bandwidth as soon as you start uploading data by avoiding unnecessary re-partitioning of the table.
* Don't insert data in separate transactions for each row. It's more efficient to insert multiple rows at once (batch inserts). This reduces the overhead on the transaction mechanism itself.
* In addition to the previous step, within each transaction (batch), insert rows from the primary key-sorted set of data to minimize the number of partitions that the transaction affects.
* Avoid writing data sequentially in ascending or descending order of the primary key value to evenly distribute the load across all table partitions.

For more information, see [Uploading large data volumes](../../best_practices/batch_upload.md).

#### How do I delete expired data? {#ttl}

To effectively remove expired data, we recommend using [TTL](../../concepts/ttl.md).


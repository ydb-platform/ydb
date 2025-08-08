# Query execution

This article provides an overview of query execution in {{ ydb-short-name }}. It is intended to familiarize users with the abilities and limitations of YDB’s query execution engine, its key features such as supported query language and execution flow. The article also introduces essential terminology and concepts related to query processing, which will be used throughout the rest of the documentation.

{{ ydb-short-name }} provides a unified query interface capable of efficiently handling diverse workloads — from high-throughput OLTP (Online Transaction Processing) to large-scale analytical OLAP (Online Analytical Processing) queries. With this approach, applications can run transactional and analytical queries transparently, without having to choose different APIs for different workloads.

{{ ydb-short-name }} uses a distributed query execution engine designed for high scalability and efficiency in large, distributed environments. When you run a query, {{ ydb-short-name }} automatically breaks down the work across multiple nodes, taking advantage of data locality — it processes data where it’s stored whenever possible. This approach reduces unnecessary data movement across the network. Additionally, {{ ydb-short-name }} leverages advanced features like compute pushdowns, where filters and computations are pushed closer to the data storage layer, further improving performance. These techniques enable {{ ydb-short-name }} to efficiently handle complex queries and large workloads across clusters of machines.

## Query language

Queries for {{ ydb-short-name }} are written in YQL — an SQL dialect, designed with distributed, scalable databases in mind. While YQL is not fully ANSI SQL compatible, it closely follows familiar SQL syntax and concepts for most common use cases, making it easy to learn for those with SQL experience. The full language reference is available in the [YQL documentation](../yql/reference/index.md).

Most interactions with {{ ydb-short-name }} are performed using YQL, making it the primary tool for querying and managing data in YDB. Because of this, understanding YQL’s features and capabilities is essential for effectively working with {{ ydb-short-name }}. Learning YQL enables you to take full advantage of the database’s advanced query functionality, express complex business logic, and utilize YDB’s distributed architecture efficiently.

YQL supports most common SQL constructs, including:

* DML — `SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `UPSERT`.
* DDL — `CREATE`, `ALTER`, `DROP` for tables, indexes, and other schema objects.
* JOINs — all standard join types plus special joins such as `LEFT/RIGHT SEMI` and `ANY` joins.
* Aggregations — `GROUP BY` and window functions.
* Named expressions for better query text organization.
* Collection of built-in functions for processing various data types, empowering users to handle complex logic directly in queries.
* Pragmas and hints to fine-tune execution plans.

## Transactions

Every query in {{ ydb-short-name }} is executed within the context of a transaction, ensuring data consistency and reliability. Transactions can be managed with `BeginTransaction`/`CommitTransaction` calls, or by providing the appropriate flags when calling the `ExecuteQuery` method.

{{ ydb-short-name }} also supports interactive transactions, which give you the flexibility to execute multiple queries within the same transaction, while allowing your application to perform custom logic between those queries. This makes it possible to build complex workflows that require several related operations to be treated as a single atomic unit.

Supported transaction modes:

* `SerializableReadWrite` — fully isolated read-write transactions (default).
* `SnapshotReadOnly` — read-only queries executed on a consistent snapshot of the data.
* `StaleReadOnly` — read-only queries that may read from replicas and therefore return slightly stale data.

For more information about transactions in {{ ydb-short-name }}, see [Transactions](transactions.md) section.

## Sessions

A session in {{ ydb-short-name }} is a logical "connection" to the database that maintains the context needed to execute queries and manage transactions. Sessions store transaction state and other important context, making it possible to execute a series of related queries as part of a transaction. Most of operations related to query execution are executed within the context of an active session.

Sessions are designed to be long-living objects. One of their key roles is to enable efficient load balancing: by distributing sessions and their associated queries across different nodes in the cluster, YDB can make better use of resources and achieve high availability and scalability.

In practice, you don’t need to worry about creating, reusing, or closing sessions yourself. All official YDB SDKs provide session pooling out of the box. A session pool automatically manages the lifecycle of sessions—creating them when needed, reusing existing ones, and returning them to the pool—so that you can focus on writing your application’s logic rather than handling session management details.

## Result Sets

When you execute a query in {{ ydb-short-name }}, the result can consist of one or more result sets. Each result set is similar to a table: it contains rows and columns, where every column has a defined, explicit data type. This strong typing guarantees that the structure of the returned data is always predictable and consistent.

Result sets in YDB can be arbitrarily large. To efficiently handle large amounts of data, YDB streams result sets back to the client in parts (chunks). This streaming approach lets clients begin processing the results right away without waiting for the entire result set to be transferred. As a result, applications can handle large datasets quickly and with minimal memory usage.

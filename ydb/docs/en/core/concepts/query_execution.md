# Query execution

This article provides an overview of query execution in {{ ydb-short-name }}. It is intended to familiarize users with the abilities and limitations of YDB’s query execution engine, its key features such as supported query language and execution flow. The article also introduces essential terminology and concepts related to query processing, which will be used throughout the rest of the documentation.

{{ ydb-short-name }} provides a unified query interface capable of efficiently handling diverse workloads — from high-throughput OLTP (Online Transaction Processing) to large-scale analytical OLAP (Online Analytical Processing) queries. With this approach, applications can run transactional and analytical queries transparently, without having to choose different APIs for different workloads.

{{ ydb-short-name }} uses a distributed query execution engine designed for high scalability and efficiency in large, distributed environments. When you run a query, {{ ydb-short-name }} automatically breaks down the work across multiple nodes, taking advantage of data locality — it processes data where it’s stored whenever possible. This approach reduces unnecessary data movement across the network. Additionally, {{ ydb-short-name }} leverages advanced features like compute pushdowns, where filters and computations are pushed closer to the data storage layer, further improving performance. These techniques enable {{ ydb-short-name }} to efficiently handle complex queries and large workloads across clusters of machines.

## General Workflow

This section provides a step-by-step overview of how SQL queries are handled in {{ ydb-short-name }}. Understanding this process will help you familiarize yourself with {{ ydb-short-name }} components and have some insight on what’s happening under the hood.

1. Connecting to the Database
Your application uses one of the official YDB SDKs to connect to the database. The SDK automatically manages a pool of sessions, logical connections required to execute queries. Behind the scenes, each session is physically connected to one of the nodes in the YDB  cluster. When you need to run a query, the SDK provides a session from this pool so you don’t need to manage connections manually.

2. Starting a Transaction and Sending a Query:
With your session in hand, your application can begin a new transaction. You then generate your query in YQL query language based on your application logic, and send this query to the YDB server using the session.

3. Parsing and Plan Cache Lookup:
On the server side, the YDB node that receives your query first parses and analyzes it for correctness. Before planning execution, YDB checks if it has already saved a physical execution plan for this query in its query cache. If it finds a cached plan, that plan can be reused to save time and resources.

4. Query Optimization and Plan Preparation:
If there isn’t an existing plan in the cache, YDB’s query optimizer creates a new physical query plan. This plan determines the most efficient way to execute your query across the distributed cluster. For more detailed information about query optimization and query plans, see [Query Optimizer](optimizer.md) article.

5. Distributed Query Execution:
Using the prepared physical plan, YDB starts distributed execution of the query. Work is distributed across multiple nodes in the database, each node undertakes a part of the computation or data access based on the plan. This parallel processing enables fast and scalable query execution, even on large datasets.

6. Streaming Results Back to the Client:
Query results are returned to your application as one or more result sets, which look like strongly typed tables. Instead of sending all results at once, YDB streams the data back in portions (parts). This allows your application to start processing results immediately and handle large result sets efficiently without needing to load everything into memory at once.

7. Continuing or Completing the Transaction:
After receiving and processing the results, your application can choose to continue the transaction by sending more queries to the transaction, or finish the transaction by committing (to save changes).

Further details and explanations of the concepts introduced in this section are provided in the following sections.

## Sessions

A session in {{ ydb-short-name }} is a logical "connection" to the database that maintains the context needed to execute queries and manage transactions. Sessions store transaction state and other important context, making it possible to execute a series of related queries as part of a transaction. Most of operations related to query execution are executed within the context of an active session.

Sessions are designed to be long-living objects. One of their key roles is to enable efficient load balancing: by distributing sessions and their associated queries across different nodes in the cluster, YDB can make better use of resources and achieve high availability and scalability.

In practice, you don’t need to worry about creating, reusing, or closing sessions yourself. All official YDB SDKs provide session pooling out of the box. A session pool automatically manages the lifecycle of sessions—creating them when needed, reusing existing ones, and returning them to the pool—so that you can focus on writing your application’s logic rather than handling session management details.

## Transactions

Every query in {{ ydb-short-name }} is executed within the context of a transaction, ensuring data consistency and reliability. Transactions can be managed with `BeginTransaction`/`CommitTransaction` calls, or by providing the appropriate flags when calling the `ExecuteQuery` method.

{{ ydb-short-name }} also supports interactive transactions, which give you the flexibility to execute multiple queries within the same transaction, while allowing your application to perform custom logic between those queries. This makes it possible to build complex workflows that require several related operations to be treated as a single atomic unit.

Supported transaction modes:

* `SerializableReadWrite` — fully isolated read-write transactions (default).
* `SnapshotReadOnly` — read-only queries executed on a consistent snapshot of the data.
* `StaleReadOnly` — read-only queries that may read from replicas and therefore return slightly stale data.

For more information about transactions in {{ ydb-short-name }}, see [Transactions](transactions.md) article.

## Retries
YDB employs an optimistic concurrency control approach for transaction management. This means that a transaction may be aborted during execution if YDB detects a conflict and cannot guarantee the requested isolation level — for instance, when two transactions attempt to modify the same data concurrently. Also, because YDB operates as a distributed system across potentially large clusters, there is also a possibility of temporary unavailability of some nodes due to network partitions, hardware failures, or maintenance. These events may also cause transaction failures that require retries.

Retries should always be handled at the transaction level, not at the level of individual queries. This is because, particularly in interactive transactions, where the sequence of queries and their intermediate results may affect subsequent operations, it is often not possible or safe to retry only a single query after a failure. Therefore, if a query fails due to a conflict or a transient error, the entire transaction should be retried from the beginning to ensure correctness and consistency.

All official YDB SDKs offer built-in retry logic and helpers for transaction management abstractions to simplify application code. By using the standard transaction methods provided by your SDK, you automatically get correct and robust retry behavior without needing to implement it yourself. For specifics about retries in SDK, please see SDK reference for your programming language.

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

## Result Sets

When you execute a query in {{ ydb-short-name }}, the result can consist of one or more result sets. Each result set is similar to a table: it contains rows and columns, where every column has a defined, explicit data type. This strong typing guarantees that the structure of the returned data is always predictable and consistent.

Result sets in YDB can be arbitrarily large. To efficiently handle large amounts of data, YDB streams result sets back to the client in parts (chunks). This streaming approach lets clients begin processing the results right away without waiting for the entire result set to be transferred. As a result, applications can handle large datasets quickly and with minimal memory usage.

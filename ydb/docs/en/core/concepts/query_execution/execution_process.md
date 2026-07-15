# Query execution process

This article describes the query execution process in {{ ydb-short-name }}. It is intended to familiarize users with the capabilities and limitations of the {{ ydb-short-name }} query execution engine, including key features such as the supported query language and the overall execution order. It also introduces basic terminology and concepts that are used in other sections of the documentation.

{{ ydb-short-name }} provides a unified interface for executing queries, capable of efficiently handling a wide range of workloads—from high‑throughput transactional OLTP queries ( [transactional OLTP queries](https://ru.wikipedia.org/wiki/OLTP)) to complex analytical OLAP queries ([analytical OLAP queries](https://ru.wikipedia.org/wiki/OLAP)). This approach allows applications to execute transactional and analytical queries transparently, without needing to use different APIs for different workload types.

A distributed engine designed for scalability and efficiency in large distributed environments is used to execute queries. When a query is launched, {{ ydb-short-name }} automatically distributes work across multiple nodes, maximizing data locality—processing data where it is stored. This reduces redundant network transfers. Additionally, computational pushdown (pushing filtering and calculations closer to the storage layer) is applied, further accelerating processing. Thanks to these techniques, {{ ydb-short-name }} efficiently handles complex queries and heavy workloads at the cluster level.

## Overall workflow

Below is a step‑by‑step description of the SQL query processing in {{ ydb-short-name }}. Understanding this process helps you better comprehend the architecture and inner workings of {{ ydb-short-name }}.

! [Query execution process](%E2%9F%A6S1%E2%9F%A7 "Query execution process")

1. **Connecting to the database**
   The application uses one of the [official {{ ydb-short-name }} SDKs](../../reference/ydb-sdk/index.md) to connect to the database. The SDK automatically manages a session pool—logical connections required for executing queries. Each session is physically tied to one of the cluster's nodes. When a query needs to be executed, the SDK provides a ready session from the pool, freeing the developer from manually managing connections.
2. **Starting a transaction and sending a query**
   Using an active session, the application can begin a transaction and formulate a query in the [YQL](../../yql/reference/index.md) language according to its business logic, then send it to the {{ ydb-short-name }} cluster.
3. **Parsing and plan cache lookup**
   On the server side, the {{ ydb-short-name }} node that received the query first validates it (parsing and analysis). Then the system checks for a ready physical execution plan in the query cache. If a plan is found, it is reused.
4. **Optimization and plan preparation**
   If no suitable plan exists, the [query optimizer](optimizer.md) creates a new physical plan that defines the most efficient way to execute the query in a distributed system. For more details on query optimization principles and plan types, see the article [{#T}](optimizer.md).
5. **Distributed query execution**
   According to the prepared physical plan, {{ ydb-short-name }} starts distributed query execution: processing is split among multiple nodes, each responsible for its portion of calculations or data access as defined by the plan. This parallelism provides high speed and scalability even for large result sets.
6. **Streaming results to the client**
   If the query returns a result (`SELECT` etc.), it is delivered to the application as one or more result sets that are strictly typed tables. Data is streamed (in chunks), allowing you to process results as they arrive and work efficiently with large data sets without loading the entire set into memory.
7. **Continuing or completing the transaction**
   After receiving the results, the application can either continue the transaction by sending additional queries in its context, or complete it by committing the changes (commit).

A more detailed description of the listed stages and related concepts is provided in the separate sections below.

## Sessions {#sessions}

A session in {{ ydb-short-name }} is a logical "connection" to the database that stores the context required for executing queries and managing transactions. Within a session, the state of transactions and other working information is maintained, allowing related queries to be executed as part of a single transaction. Most query operations are performed in the context of an active session.

Sessions are long‑living objects. One of their important tasks is efficient load distribution: by distributing sessions and their associated queries across different nodes, the {{ ydb-short-name }} cluster achieves high availability and scalability.

In practice, you do not need to create, reuse, or delete sessions manually. All official SDKs for {{ ydb-short-name }} provide a built‑in session pool: the SDK manages the session lifecycle itself, creates sessions as needed, reuses them, and returns them to the pool—this is transparent to the user and does not require additional application logic.

## Transactions

Each query in YDB is executed within a transaction context, ensuring consistency and reliable data storage. Transactions can be managed explicitly (through separate SDK calls) or by specifying appropriate flags during query execution.

YDB also supports [interactive transactions](../glossary.md#interactive-transaction), which allow you to execute multiple queries within a single transaction, enabling your application to run custom logic between those queries. This makes it possible to build complex workflows that treat several related operations as a single atomic unit.

For detailed information about transactions and available transaction modes in {{ ydb-short-name }}, see the article [Transactions](../transactions.md).

## Retries

In {{ ydb-short-name }}, the [optimistic locking](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) mechanism is used for transaction management. This means that a transaction can be aborted during execution if a conflict is detected and the system cannot guarantee the required isolation level—for example, if two transactions modify the same data simultaneously. In addition to conflicts, a distributed environment may experience temporary unavailability of individual nodes due to network failures, hardware faults, or maintenance, which can also necessitate retrying the transaction.

Retry logic should always be implemented at the transaction level, not per individual query. In [interactive transactions](../glossary.md#interactive-transaction), the execution order and intermediate results of individual queries can affect subsequent actions. Therefore, if a query fails due to a conflict or a temporary error, you must retry the entire transaction from the beginning to ensure data correctness and consistency.

All official SDKs for {{ ydb-short-name }} provide built‑in transaction management and retry mechanisms that simplify application development. By using the SDK’s standard transaction retry mechanisms, you automatically get a correct retry‑logic implementation without having to implement it manually. For more details about retry mechanisms for various SDKs, see [{#T}](../../reference/ydb-sdk/error_handling.md).

## Query language

Queries for {{ ydb-short-name }} are written in [YQL](../glossary.md#yql) — an SQL dialect specially adapted for distributed, scalable databases. Although YQL is not fully compatible with ANSI SQL, it largely retains the familiar SQL syntax and principles, making it easier for experienced SQL users to learn and transition. The full language reference is provided in the [YQL documentation](../../yql/reference/index.md).

Most data operations in {{ ydb-short-name }} are performed through YQL—it is the primary tool for data manipulation and database administration. Mastering YQL lets you leverage all capabilities of the distributed architecture {{ ydb-short-name }} and implement complex business logic directly in queries.

YQL supports all major SQL constructs, including:

- [Data Manipulation Language (DML)](https://ru.wikipedia.org/wiki/%D0%AF%D0%B7%D1%8B%D0%BA_%D0%BC%D0%B0%D0%BD%D0%B8%D0%BF%D1%83%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F_%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%BC%D0%B8): `SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `UPSERT`.
- [Data Definition Language (DDL)](https://ru.wikipedia.org/wiki/%D0%AF%D0%B7%D1%8B%D0%BA_%D0%BE%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F_%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85): `CREATE`, `ALTER`, `DROP` for tables, indexes, and other schema objects.
- Joins — all standard types of joins, as well as special join types (e.g., `LEFT SEMI`, `RIGHT SEMI`, `ANY`).
- Aggregations — grouping (`GROUP BY`) and window functions.
- [named expressions](../../yql/reference/syntax/expressions.md#named-nodes) for structuring query text.
- A large number of built-in functions for processing different data types and solving complex tasks directly in the query.
- Pragmas (pragma) and hints (hints) for controlling the execution plan.

## Result sets

The result of executing a query in {{ ydb-short-name }} can be one or multiple result sets. A result set resembles a table: it contains rows with strict data typing in each column. Strict typing of results ensures predictability and consistency of the output format.

Result sets can contain an arbitrarily large amount of data, so for efficient transfer {{ ydb-short-name }} uses streaming output (streaming) — the result is returned to the client in chunks. This allows you to start processing the data immediately without waiting for the entire result set and minimizes memory usage by the client application.

## Limits

When working with queries in {{ ydb-short-name }}, it is important to consider a number of limitations:

* **No schema transactions**
  {{ ydb-short-name }} does not support schema transactions, so DDL operations (creating or modifying tables) cannot be combined with DML queries (inserting, modifying, or deleting data) in a single transaction or a single query.
* **Large updates and optimistic locks**
  {{ ydb-short-name }} uses an optimistic locking mechanism. When you try to execute very large `UPDATE` or `DELETE` within a single transaction, the likelihood of lock conflicts increases sharply, making these operations impractical for real‑world use. For large changes, it is recommended to use [`BATCH UPDATE`](../../yql/reference/syntax/batch-update.md) or [`BATCH DELETE`](../../yql/reference/syntax/batch-delete.md).
* **Transaction size limits**
  The volume of data written in a single transaction is limited. See details in the [{#T}](../limits-ydb.md#query) section.

The full list of system limitations is provided in [{#T}](../limits-ydb.md).

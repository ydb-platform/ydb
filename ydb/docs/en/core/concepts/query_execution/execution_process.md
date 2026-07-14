# Query execution process

This article describes the query execution process in {{ ydb-short-name }}. It is intended to familiarize users with the capabilities and limitations of the {{ ydb-short-name }} query execution engine, including key features such as the supported query language and the general order of execution. It also introduces basic terminology and concepts used in other sections of the documentation.

{{ ydb-short-name }} provides a unified interface for query execution, capable of efficiently handling a wide range of workloads — from high-load [transactional OLTP queries](https://en.wikipedia.org/wiki/Online_transaction_processing) to complex [analytical OLAP queries](https://en.wikipedia.org/wiki/Online_analytical_processing). This approach allows applications to execute transactional and analytical queries transparently, without needing to use different APIs for different workload types.

A distributed engine designed for scalability and efficiency in large distributed environments is used for query execution. When a query is launched, {{ ydb-short-name }} automatically distributes the work across multiple nodes, maximizing data locality — processing data where it is stored. This reduces unnecessary network transfers. Additionally, computational pushdown (moving filtering and computations closer to the storage layer) is applied, which further speeds up processing. Thanks to these techniques, {{ ydb-short-name }} efficiently handles complex queries and large loads at the cluster level.

## General workflow

The following describes the step-by-step process of processing SQL queries in {{ ydb-short-name }}. Understanding this process helps to better understand the architecture and internal structure of {{ ydb-short-name }}.

! [Query execution process](%E2%9F%A6S1%E2%9F%A7 "Query execution process")

1. **Connecting to the database**
   The application uses one of the [official {{ ydb-short-name }} SDKs](../../reference/ydb-sdk/index.md) to connect to the database. The SDK automatically manages a pool of sessions — logical connections required for query execution. Each session is physically associated with one of the cluster nodes. When a query needs to be executed, the SDK provides a ready session from the pool, freeing the developer from manual connection management.
2. **Starting a transaction and sending a query**
   Using an active session, the application can start a transaction and formulate a query in [YQL](../../yql/reference/index.md) according to its business logic, and then send it to the {{ ydb-short-name }} cluster.
3. **Parsing and checking the plan cache**
   On the server side, the {{ ydb-short-name }} node that received the query first checks its correctness (parsing and analysis). Then the system checks for an existing physical execution plan in the query cache. If a plan is found, it is reused.
4. **Optimization and plan preparation**
   If no suitable plan exists, the [query optimizer](optimizer.md) creates a new physical plan that determines the most efficient way to execute the query in the distributed system. For more details on query optimization principles and plan types, see the article [{#T}](optimizer.md).
5. **Distributed query execution**
   According to the prepared physical plan, {{ ydb-short-name }} starts distributed query execution: processing is divided among multiple nodes, each responsible for its part of the calculations or data access according to the received plan. This parallelism ensures high speed and scalability even for large data sets.
6. **Streaming results to the client**
   If the query returns a result (`SELECT`, etc.), it arrives at the application as one or more result sets, which are strictly typed tables. Data is transferred in a streaming fashion (in chunks), allowing results to be processed as they arrive and efficiently handle large data sets without loading the entire set into memory.
7. **Continuing or completing the transaction**
   After receiving the results, the application can either continue the transaction by sending additional queries in its context, or complete it by committing changes (commit).

A more detailed description of the listed stages and related concepts is provided in the sections below.

## Sessions {#sessions}

A session in {{ ydb-short-name }} is a logical "connection" to a database that stores the context required for executing queries and managing transactions. Inside a session, the state of transactions and other working information is maintained, allowing related queries to be executed as part of a single transaction. Most query operations are performed in the context of an active session.

Sessions are long-lived objects. One of their important tasks is efficient load distribution: by distributing sessions and their associated queries across different cluster nodes, {{ ydb-short-name }} achieves high availability and scalability.

In practice, there is no need to manually create, reuse, and delete sessions. All official SDKs for {{ ydb-short-name }} provide a built-in session pool: the SDK itself manages the session lifecycle, creates them as needed, reuses them, and returns them back to the pool — all of this is transparent to the user and does not require additional logic in the application.

## Transactions

Each query in YDB is executed in the context of a transaction, which ensures consistency and reliable data storage. Transactions can be managed explicitly (through separate SDK calls) or by specifying the appropriate flags during query execution.

YDB also supports [interactive transactions](../glossary.md#interactive-transaction), which allow you to execute multiple queries within a single transaction, while enabling your application to run custom logic between these queries. This allows building complex workflows where multiple related operations need to be treated as a single atomic unit.

For detailed information about transactions and available transaction modes in {{ ydb-short-name }}, see the article [Transactions](../transactions.md).

## Retries

In {{ ydb-short-name }}, the mechanism of [optimistic locks](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) is used to manage transactions. This means that a transaction can be interrupted during execution if a conflict is detected and the system cannot guarantee the required isolation level — for example, if two transactions simultaneously modify the same data. In addition to conflicts, in a distributed environment, individual nodes may temporarily become unavailable due to network failures, hardware failures, or maintenance, which may also require the transaction to be retried.

Retry should always be implemented at the level of the entire transaction, rather than an individual query. In [interactive transactions](../glossary.md#interactive-transaction), the execution sequence and intermediate results of individual queries can affect subsequent actions. Therefore, if a query fails due to a conflict or a temporary error, it is necessary to retry the entire transaction from the beginning to ensure data correctness and consistency.

All official SDKs for {{ ydb-short-name }} provide built-in transaction and retry management mechanisms that simplify application development. By using the standard retry mechanisms from the SDK, you automatically get a correct implementation of retry logic without having to implement it manually. For more information about retry mechanisms for various SDKs, see [{#T}](../../reference/ydb-sdk/error_handling.md).

## Query language

Queries for {{ ydb-short-name }} are written in [YQL](../glossary.md#yql), an SQL dialect specially adapted for distributed scalable databases. Although YQL is not fully compatible with ANSI SQL, it largely retains familiar SQL syntax and principles, making it easier to learn and transition for experienced SQL users. The complete language reference is provided in the [YQL documentation](../../yql/reference/index.md).

Most data operations in {{ ydb-short-name }} are performed using YQL — it is the main tool for working with data and administering the database. Proficiency in YQL allows you to leverage all the capabilities of {{ ydb-short-name }}'s distributed architecture and implement complex business logic directly in queries.

YQL supports all basic SQL constructs, including:

- [Data Manipulation Language (DML)](https://en.wikipedia.org/wiki/%D0%AF%D0%B7%D1%8B%D0%BA_%D0%BC%D0%B0%D0%BD%D0%B8%D0%BF%D1%83%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F_%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%BC%D0%B8): `SELECT`, `INSERT`, `REPLACE`, `UPDATE`, `DELETE`, `UPSERT`.
- [Data Definition Language (DDL)](https://en.wikipedia.org/wiki/%D0%AF%D0%B7%D1%8B%D0%BA_%D0%BE%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F_%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85): `CREATE`, `ALTER`, `DROP` for tables, indexes, and other schema objects.
- Joins: all standard join types, as well as special join types (for example, `LEFT SEMI`, `RIGHT SEMI`, `ANY`).
- Aggregations — grouping (`GROUP BY`) and window functions.
- [Named expressions](../../yql/reference/syntax/expressions.md#named-nodes) for structuring query text.
- A large number of built-in functions for processing different data types and solving complex tasks directly in the query.
- Pragmas and hints for managing the execution plan.

## Result sets

The result of executing a query in {{ ydb-short-name }} can be one or more result sets. A result set is similar to a table: it contains rows with strict data typing in each column. Strict typing of results ensures predictability and consistency of the output format.

Result sets can contain arbitrarily large amounts of data, so for their efficient transfer {{ ydb-short-name }} uses streaming — the result is returned to the client in chunks. This allows you to start processing data immediately without waiting for the entire result set to be received and minimizes the use of RAM by the client application.

## Limitations

When working with queries in {{ ydb-short-name }}, it is important to consider a number of limitations:

* **No schema transactions**
  {{ ydb-short-name }} does not support schema transactions, so DDL operations (creating or modifying tables) cannot be combined with DML queries (inserting, modifying, or deleting data) in a single transaction or query.
* **Large updates and optimistic locking**
  {{ ydb-short-name }} uses an optimistic locking mechanism. When attempting to execute very large `UPDATE` or `DELETE` within a single transaction, the likelihood of lock conflicts increases significantly, making such operations impractical for real use. For large changes, it is recommended to use [`BATCH UPDATE`](../../yql/reference/syntax/batch-update.md)/[`BATCH DELETE`](../../yql/reference/syntax/batch-delete.md).
* **Transaction size limits**
  The amount of data written in a single transaction is limited. For details, see the [{#T}](../limits-ydb.md#query) section.

The full list of system limitations is given in [{#T}](../limits-ydb.md).

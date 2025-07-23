# {{ ydb-short-name }} Transactions and Queries

This section describes the specifics of YQL implementation for {{ ydb-short-name }} transactions.

## Query Language {#query-language}

The main tool for creating, modifying, and managing data in {{ ydb-short-name }} is a declarative query language called YQL. YQL is an SQL dialect that can be considered a database interaction standard. {{ ydb-short-name }} also supports a set of special RPCs useful in managing a tree schema or a cluster, for instance.

## Transaction Modes {#modes}

By default, {{ ydb-short-name }} transactions are executed in *Serializable* mode. It provides the strictest [isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable) for custom transactions. This mode guarantees that the result of successful parallel transactions is equivalent to their serial execution, and there are no [read anomalies](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_phenomena) for successful transactions.

If consistency or freshness requirement for data read by a transaction can be relaxed, a user can take advantage of execution modes with lower guarantees:

* *Online Read-Only*: Each read operation in the transaction is reading the data that is most recent at execution time. The consistency of retrieved data depends on the *allow_inconsistent_reads* setting:

  * *false* (consistent reads): Each individual read operation returns consistent data, but no consistency is guaranteed between reads. Reading the same table range twice may return different results.
  * *true* (inconsistent reads): Even the data fetched by a particular read operation may contain inconsistent results.

* *Stale Read-Only*: Read operations within a transaction may return results that are slightly out-of-date (lagging by fractions of a second). Each individual read returns consistent data, but no consistency between different reads is guaranteed.
* *Snapshot Read-Only*: All the read operations within a transaction access the database snapshot. All the data reads are consistent. The snapshot is taken when the transaction begins, meaning the transaction sees all changes committed before it began.

### Implicit Transactions in {{ ydb-short-name }} {#implicit}

When a transaction is not explicitly defined for a query, {{ ydb-short-name }} automatically manages transactional behavior. This is referred to as an implicit transaction. Unlike explicit transactions, where applications initiate and commit transactions directly, implicit transactions are internally handled by the {{ ydb-short-name }} server when no explicit transaction mode is specified.

**Behavior and Guarantees**

The guarantees provided by implicit transactions are generally weaker than those offered by explicit transactions. In particular, some types of operations executed via implicit transactions cannot be rolled back if an error occurs. The behavior varies depending on the type of statement:

- **Data Definition Language (DDL) Statements**
  DDL queries (such as `CREATE TABLE`, `DROP TABLE`, etc.) are always executed outside of any transaction context. Multistatement queries are supported only if all statements are DDL. If an error occurs, changes made by previous statements within the batch are not rolled back.

- **Data Manipulation Language (DML) Statements**
  DML queries (such as `INSERT`, `UPDATE`, `DELETE`, etc.) are automatically executed inside a transaction with Snapshot (for read-only queries) or Serializable (for read-write queries) isolation, created and managed by the server. Multistatement queries are supported if all statements are DML. On successful execution, changes are committed. If an error occurs, all changes are rolled back.

- **BATCH UPDATE / BATCH DELETE FROM Statements**
  Batch modification statements are executed outside of a transactional context. Multistatement queries are not supported. As with DDL, changes are not rolled back if an error occurs during execution.

**Summary Table**

| Statement Type | Implicit Transaction Handling                 | Multistatement Support | Rollback on Error     |
|----------------|-----------------------------------------------|------------------------|-----------------------|
| DDL            | Outside transaction context                   | Yes (DDL-only)         | No                    |
| DML            | Auto transaction (Serializable/Snapshot)      | Yes (DML-only)         | Yes                   |
| BATCH UPDATE / BATCH DELETE FROM | Outside transaction context | No                     | No                    |

When using implicit transactions, be aware of these limitations and reduced guarantees compared to explicit transactions. For advanced transaction control, error handling, or atomicity across diverse statements, it is strongly recommended to use explicit transactions.

The transaction execution mode is specified in its settings when creating the transaction. See the examples for the {{ ydb-short-name }} SDK in the [{#T}](../../recipes/ydb-sdk/tx-control.md).

---

## YQL Language {#language-yql}

Statements implemented in YQL can be divided into two classes: [Data Definition Language (DDL)](https://en.wikipedia.org/wiki/Data_definition_language) and [Data Manipulation Language (DML)](https://en.wikipedia.org/wiki/Data_manipulation_language).

For more information about supported YQL constructs, see the [YQL documentation](../../yql/reference/index.md).

Listed below are the features and limitations of YQL support in {{ ydb-short-name }}, which might not be obvious at first glance and are worth noting:

* Multi-statement transactions (transactions made up of a sequence of YQL statements) are supported. Transactions may interact with client software, or in other words, client interactions with the database might look as follows: `BEGIN; make a SELECT; analyze the SELECT results on the client side; ...; make an UPDATE; COMMIT`. We should note that if the transaction body is fully formed before accessing the database, it will be processed more efficiently.
* {{ ydb-short-name }} does not support transactions that combine DDL and DML queries. The conventional [ACID](https://en.wikipedia.org/wiki/ACID) notion of a transaction is applicable specifically to DML queries, that is, queries that change data. DDL queries must be idempotent, meaning repeatable if an error occurs. If you need to manipulate a schema, each manipulation is transactional, while a set of manipulations is not.
* YQL implementation used in {{ ydb-short-name }} employs the [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) mechanism. If an entity is affected during a transaction, optimistic blocking is applied. When the transaction is complete, the mechanism verifies that the locks have not been invalidated. For the user, locking optimism means that when transactions are competing with one another, the one that finishes first wins. Competing transactions fail with the `Transaction locks invalidated` error.
* All changes made during the transaction accumulate in the database server memory and are applied when the transaction completes. If the locks are not invalidated, all the changes accumulated are committed atomically; if at least one lock is invalidated, none of the changes are committed. The above model involves certain restrictions: changes made by a single transaction must fit inside the available memory.

For efficient execution, a transaction should be formed so that the first part of the transaction only reads data, while the second part of the transaction only changes data. The query structure then looks as follows:

```yql
       SELECT ...;
       ....
       SELECT ...;
       UPDATE/REPLACE/DELETE ...;
       COMMIT;
```

For more information about YQL support in {{ ydb-short-name }}, see the [YQL documentation](../../yql/reference/index.md).

## Distributed Transactions {#distributed-tx}

A database [table](../datamodel/table.md) in {{ ydb-short-name }} can be sharded by the range of the primary key values. Different table shards can be served by different distributed database servers (including ones in different locations). They can also move independently between servers to enable rebalancing or ensure shard operability if servers or network equipment goes offline.

A [topic](../topic.md) in {{ ydb-short-name }} can be sharded into several partitions. Different topic partitions, similar to table shards, can be served by different distributed database servers.

{{ ydb-short-name }} supports distributed transactions. Distributed transactions are transactions that affect more than one shard of one or more tables and topics. They require more resources and take more time. While point reads and writes may take up to 10 ms in the 99th percentile, distributed transactions typically take from 20 to 500 ms.

## Transactions with Topics and Tables {#topic-table-transactions}

{{ ydb-short-name }} supports transactions involving [row-oriented tables](../glossary.md#row-oriented-table) and/or [topics](../glossary.md#topic). This makes it possible to transactionally transfer data from tables to topics and vice versa, as well as between topics. This ensures that data is neither lost nor duplicated in case of a network outage or other issues. This enables the implementation of the transactional outbox pattern within {{ ydb-short-name }}.

For more information about transactions with tables and topics in {{ ydb-short-name }}, see [{#T}](../topic.md#topic-transactions) and [{#T}](../../reference/ydb-sdk/topic.md).
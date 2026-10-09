# Transactions and queries to {{ ydb-short-name }}

This section describes the specifics of YQL implementation for {{ ydb-short-name }} transactions.

## Query language {#query-language}

The main tool for creating, modifying, and managing data in {{ ydb-short-name }} is the declarative query language YQL. YQL is a SQL dialect that can be considered a standard for communicating with databases. In addition, {{ ydb-short-name }} supports a set of special RPCs, for example, for working with a tree schema or for managing a cluster.

## Transaction modes {#modes}

{{ ydb-short-name }} supports several transaction execution modes. By default, transactions are executed in *Serializable* mode, which provides the strictest [isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable) for user transactions. The transaction execution mode is set in the settings when it is created. Examples for {{ ydb-short-name }} SDK see in [{#T}](../../recipes/ydb-sdk/tx-control.md). In [{{ ydb-ui-name }}](../../reference/ydb-ui/index.md) the transaction mode can also be selected in the settings.

Supported **read-write modes**: *Serializable* (default) and *Snapshot Read-Write*; **read-only modes**: *Snapshot Read-Only* and *Stale Read-Only*. The *Online Read-Only* mode is left for compatibility with old code (**legacy**); in new scenarios, use *Snapshot Read-Only* instead.

### Serializable {#serializable}

**Essence.** Serializable execution of user transactions (Serializable [isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable)).

**Features.** The [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) mechanism is used. Optimistic locks are placed on rows read during the transaction. When the transaction completes, it is checked that the locks have not been invalidated. The optimistic nature of locks results in an important property for the user — in case of a conflict, the transaction that completes first wins. Competing transactions will fail with an error `Transaction locks invalidated`.

**Guarantees.**

* The result of successfully executed parallel transactions is equivalent to some serial order of their execution;
* For successful transactions, there are no [read anomalies](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_phenomena);
* A transaction sees all changes that were committed before its first read (the time the snapshot was taken), plus its own changes made earlier in the same transaction (read-your-own-writes);
* Linearizability **by key** is guaranteed: if transaction T1, affecting a certain key, completed before transaction T2, affecting **the same key**, began, then in the execution order T1 will be before T2. For transactions working with different keys, such an order is not guaranteed — the observed order of their commit may differ from the real order of their completion in time.

### Snapshot Read-Write {#snapshot-read-write}

**Essence.** It is [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) (analogous to Repeatable Read in PostgreSQL).
Reads are performed from a consistent data snapshot committed before the first read. A transaction will successfully commit only if, from the moment the data snapshot was taken until the transaction commit, the rows it modified were not modified by other transactions.

**Features.** The [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) mechanism is used. Unlike Serializable, transactions do not take locks on read rows — this allows them to commit successfully even if the data they read was modified by other transactions. Locks are taken on modified rows: with parallel transactions changing the same keys, only the transaction that completes first will succeed. Competing transactions will be rejected with a write-write conflict at the commit stage with an error `Transaction locks invalidated`.

**Guarantees.**

* All reads in a transaction see the same data state on the snapshot obtained before the first read, plus its own changes made earlier in the same transaction (read-your-own-writes);
* If there is a write-write conflict, the transaction will not be able to commit;
* The [write skew](https://en.wikipedia.org/wiki/Snapshot_isolation) anomaly may be observed.

### Snapshot Read-Only {#snapshot-read-only}

**Essence.** The transaction works with a consistent database snapshot committed before the first read. The guarantees for data reads are the same as Snapshot Read-Write. Writes are prohibited in this mode, which makes it more efficient than Snapshot Read-Write when only data reading is needed.

**Features.** Provides maximum data freshness at the start of the transaction, but may have higher response latency due to the need to form a snapshot.

**Guarantees.** All reads in a transaction see the same data state on the snapshot; commits after the snapshot is taken are not visible.

### Stale Read-Only {#stale-read-only}

**Essence.** Reads are performed on [tablet (shard) replicas](../glossary.md#tablet-follower) with possible lag behind the [tablet (shard) leader](../glossary.md#tablet-leader) (usually fractions of a second). The mode is well suited for key-based read scenarios when minimal latency is needed. The user reads committed but possibly stale data.

**Features.** Low latency and high throughput due to reading from replicas. The read replica is usually selected in the same availability zone, but the exact location is not guaranteed.

**Guarantees.** Data consistency at the key level within **one** `SELECT` expression; between **different** `SELECT` expressions in the same transaction, consistency is **not** guaranteed.

**Limitations.** There is no single snapshot for the entire transaction; there may be a delay relative to the data on the leader (the data may not be the freshest). Using replicas for reads is possible only if all read rows are in one shard. If the read spans multiple shards, the read will be performed from leaders with a snapshot taken similarly to Snapshot Read-Only. Reading from [column-oriented tables](../datamodel/table.md#column-oriented-tables) in this mode is not supported (see the warning below). Interactive transactions are not supported. For additional limitations and nuances on query types, see the SDK documentation for the selected language.

### Online Read-Only {#online-read-only}

A deprecated (**legacy**) mode retained for compatibility. For new applications, when reading without writing, use *Snapshot Read-Only*. Details and examples of calling in the SDK can still be found in [{#T}](../../recipes/ydb-sdk/tx-control.md#online-read-only).

{% note warning "Limitation for Online Read-Only and Stale Read-Only" %}

These modes do not support reading from column-oriented tables. An attempt to read will cause an error of the following form:


```text
Read from column tables is not supported in Online Read-Only or
Stale Read-Only transaction modes. Use Serializable or
Snapshot Read-Only mode instead.
```

For transactions that read from column-oriented tables, use:

* Serializable — the default mode;
* Snapshot Read-Only — a mode for reading from a consistent snapshot.

{% endnote %}

### Implicit transactions {#implicit}

The logic of implicit transactions is applied when sending a single YQL script to the server without explicitly selecting a [transaction mode](#modes). Typical entry points:

* [{{ ydb-ui-name }}](../../reference/ydb-ui/index.md) — the **Query** tab on the database page ([query execution form](../../reference/ydb-ui/ydb-monitoring.md#tenant_scheme)), when run without selecting a transaction mode in the settings.
* [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md) — one-off script submission via the [`ydb sql`](../../reference/ydb-cli/sql.md) command.
* Applications on [{{ ydb-short-name }} SDK](../../reference/ydb-sdk/index.md) — mode [ImplicitTx](../../recipes/ydb-sdk/tx-control.md#implicittx).

If a [transaction mode](../transactions.md#modes) is not set for a query, {{ ydb-short-name }} automatically manages its behavior. This mode is called an **implicit transaction**.

In this mode, {{ ydb-short-name }} determines based on the query whether to execute it outside a transaction or wrap it in a transaction with *Serializable* mode. The implicit transaction mode is universal for query execution, as it supports statements of any kind with the specific behavior described below.

#### Behavior for different types of statements

- **[Data Definition Language](https://en.wikipedia.org/wiki/Data_definition_language) (DDL) statements**
  DDL statements (such as [`CREATE TABLE`](../../yql/reference/syntax/create_table/index.md), [`DROP TABLE`](../../yql/reference/syntax/drop_table.md), etc.) are executed outside a transaction. A query can consist only of DDL statements. If an error occurs, changes made by previous statements in the query are not rolled back.

- **[Data Manipulation Language](https://en.wikipedia.org/wiki/Data_manipulation_language) (DML) statements**
  DML statements (such as [`UPSERT`](../../yql/reference/syntax/upsert_into.md), [`SELECT`](../../yql/reference/syntax/select/index.md), [`UPDATE`](../../yql/reference/syntax/update.md), etc.) are wrapped in a transaction with *Serializable* mode. A query can consist only of DML statements. On successful execution, changes are committed, and if an error occurs, they are rolled back.

- **Batch modification statements**
  Batch modification statements (such as [`BATCH UPDATE`](../../yql/reference/syntax/batch-update.md) and [`BATCH DELETE FROM`](../../yql/reference/syntax/batch-delete.md)) are executed outside a transaction. A query can consist only of one batch modification statement. If an error occurs, the statement's changes are not rolled back.

#### Summary table

| Statement type | Implicit transaction handling                      | Support for multiple statements | Rollback on error      |
|----------------|---------------------------------------------------|---------------------------------|-----------------------|
| DDL            | Outside a transaction                             | Yes (DDL only)                  | No                    |
| DML            | Automatic transaction (Serializable)              | Yes (DML only)                  | Yes                   |
| Batch modification statements | Outside a transaction             | No                              | No                    |

To explicitly set a transaction mode, use the appropriate settings at each entry point:

* [{{ ydb-ui-name }}](../../reference/ydb-ui/index.md) — select a transaction mode in the execution settings on the **Query** tab.
* [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md) — for the subcommand [`table query execute`](../../reference/ydb-cli/table-query-execute.md) for queries of type `data` set the parameter [`--tx-mode`](../../reference/ydb-cli/table-query-execute.md#options) (default `serializable-rw`, which corresponds to *Serializable* mode).
* [{{ ydb-short-name }} SDK](../../reference/ydb-sdk/index.md) — see [setting the mode in the {{ ydb-short-name }} SDK](../../recipes/ydb-sdk/tx-control.md).

## YQL language {#language-yql}

Implemented YQL constructs can be divided into two classes: [data definition language (DDL)](https://en.wikipedia.org/wiki/Data_definition_language) and [data manipulation language (DML)](https://en.wikipedia.org/wiki/Data_manipulation_language).

For more information about supported YQL constructs, see the [YQL documentation](../../yql/reference/index.md).

Below are the features and limitations of YQL support in {{ ydb-short-name }} that are worth paying attention to:

* Multistatement transactions are allowed, that is, transactions consisting of a sequence of YQL expressions. During transaction execution, interaction with the client program is allowed; in other words, client interaction with the database may look like this: `begin a transaction and execute SELECT; analyze the SELECT results on the client; ...; execute UPDATE and commit the transaction`. Each of the queries within a transaction can also contain multiple YQL expressions. It is worth noting that if the transaction body is fully formed before accessing the database, the transaction can be processed more efficiently;
* In {{ ydb-short-name }} it is not supported to mix DDL and DML queries in one transaction. The traditional concept of an [ACID](https://en.wikipedia.org/wiki/ACID) transaction applies specifically to DML queries, that is, queries that change data. DDL queries must be idempotent, that is, repeatable in case of an error. If you need to perform an action with a schema, each action will be transactional, but a set of actions will not;
* Any errors invalidate the entire transaction as a whole, not an individual query, so after a transaction completes with an error reporting a [temporary failure](../../reference/ydb-sdk/error_handling.md), the transaction must be retried from the very beginning;
* Reads in a transaction see all data changes that were made earlier in the same transaction;
* All changes made within a transaction accumulate in the memory of the database server. They are not visible to other transactions until the current one completes successfully and are applied atomically at commit. The described scheme imposes a limitation: the volume of changes within a single transaction must fit in RAM. **Implementation detail:** if a transaction reads data from a table that it previously modified, the accumulated changes are written to shards prematurely — this affects efficiency (see the recommendation below). Prematurely written data is not visible to other transactions and is rolled back when the transaction is canceled;
* For transaction efficiency, avoid reading from tables previously modified in the same transaction (read-after-write), as this leads to premature data writes to shards. For each table, perform all reads before modifications.

For more information about YQL support in {{ ydb-short-name }} see the [YQL documentation](../../yql/reference/index.md).

## Distributed transactions {#distributed-tx}

A [table](../datamodel/table.md) in {{ ydb-short-name }} can be sharded by ranges of primary key values. Different table shards can be served by different servers of the distributed database (including those located in different locations), and can also move independently between servers for rebalancing or maintaining shard operability during server or network equipment failures.

A [topic](../datamodel/topic.md) in {{ ydb-short-name }} can be sharded into multiple partitions. Different topic partitions, like table shards, can be served by different servers of the distributed database.

In {{ ydb-short-name }} distributed transactions are supported. Distributed transactions are transactions that affect more than one shard of one or more tables and topics. They require more resources and take longer. While point reads and writes can be performed in up to 10 ms at the 99th percentile, distributed transactions typically take from 20 to 500 ms.

## Transactions involving topics and tables {#topic-table-transactions}

{% note warning %}

{% include [not_allow_for_olap](../../_includes/not_allow_for_olap_text.md) %}

{% endnote %}

{{ ydb-short-name }} supports transactions involving [row-oriented tables](../glossary.md#row-oriented-table) and/or topics. Thus, you can transactionally move data from tables to topics and in the reverse direction, as well as between topics, so that data is not lost or duplicated even in unforeseen circumstances.

For more information about transactional operations when working with topics, see [{#T}](../datamodel/topic.md#topic-transactions) and [{#T}](../../reference/ydb-sdk/topic.md).

## Transactions involving row-oriented and column-oriented tables {#mixed-transactions}

{% include [limitation](../../yql/reference/_includes/limitation-column-row-in-read-only-tx.md) %}

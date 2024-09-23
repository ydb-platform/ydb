### Migrations in YDB using “goose” {#2023-pub-medium-goose}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![Migrations in YDB using goose](./_includes/goose.webp ={{pub-cover-size}})

Any production process that works with a database will require a schema migration sooner or later. The migration updates the database’s table structure from one version to the next. Schema migrations can be done manually by executing an `ALTER TABLE` query or by using specialized tools. One such tool is called goose. In this [article](https://blog.ydb.tech/migrations-in-ydb-using-goose-58137bc5c303) we see how goose provides schema management in a project and has supported YDB (a distributed open-source database) since v3.16.0.

### About prepared statements, server-side compiled query cache, or how to efficiently cache queries in YDB {#2023-pub-medium-cache-queries}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![How to efficiently cache queries in YDB](./_includes/cache-queries.webp =700x300)

There are various ways to reduce the cost of SQL query execution in modern DBMS. The most common approaches are using prepared statements and query caching. Both methods are available in YDB. Their functionality and benefits are discussed in this [article](https://blog.ydb.tech/about-prepared-statements-server-side-compiled-query-cache-or-how-to-efficiently-cache-queries-in-df3af73eb001).

### YDB meets TPC-C: distributed transactions performance now revealed {#2023-pub-medium-tcp-c}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![YDB meets TPC-C](./_includes/tcp-c.webp ={{pub-cover-size}})

We are excited to present our first [results](https://blog.ydb.tech/ydb-meets-tpc-c-distributed-transactions-performance-now-revealed-42f1ed44bd73) of [TPC-C](https://www.tpc.org/tpcc/)*, which is industry-standard On-Line Transaction Processing (OLTP) benchmark. According to these results, there are scenarios in which YDB slightly outperforms CockroachDB, another trusted and well-known distributed SQL database.

### database/sql bindings for YDB in Go {#2023-pub-medium-ydb-go}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![Database/sql bindings for YDB in Go](./_includes/database.webp ={{pub-cover-size}})

YQL is a SQL dialect with YDB specific strict types. This is great for performance and correctness, but sometimes can be a bit daunting to express in a query, especially when they need to be parametrized externally from the application side. For instance, when a YDB query needs to be parametrized, each parameter has name and type provided via `DECLARE` statement. To explore more about this and see practical examples, read the detailed explanation in this [article](https://blog.ydb.tech/database-sql-bindings-for-ydb-in-go-a8a2671a8696).

### YCSB performance series: YDB, CockroachDB, and YugabyteDB {#2023-pub-medium-ycsb}

{% include notitle [database_internals_tag](../../tags.md#database_internals) %}

![YCSB performance series](./_includes/ycsb.webp ={{pub-cover-size}})

It’s a challenge to implement a distributed database with strong consistency, ensuring high speed and scalability. YDB excels in these aspects, and our customers can attest to this through their own experiences. Unfortunately, we have never presented any performance numbers to a broader audience. We recognize the value of this [information](https://blog.ydb.tech/ycsb-performance-series-ydb-cockroachdb-and-yugabytedb-f25c077a382b), and we are preparing more benchmark results to share.
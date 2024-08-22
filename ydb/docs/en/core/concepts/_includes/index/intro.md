---
title: "YDB. DBMS overview"
description: "YDB is a horizontally scalable distributed fault-tolerant DBMS. YDB is designed to meet high performance requirements, for example, a typical server can handle dozens of thousands of requests per second. The system is designed to handle hundreds of petabytes of data."
---

# {{ ydb-short-name }} overview

*{{ ydb-short-name }}* is a horizontally scalable distributed fault-tolerant DBMS. {{ ydb-short-name }} is designed for high performance with a typical server being capable of handling tens of thousands of queries per second. The system is designed to handle hundreds of petabytes of data. {{ ydb-short-name }} can operate in single data center and geo-distributed (cross data center) modes on a cluster of thousands of servers.

{{ ydb-short-name }} provides:

* [Strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_Consistency) which can be relaxed to increase performance.
* Support for queries written in [YQL](../../../yql/reference/index.md) (an SQL dialect for working with big data).
* Automatic data replication.
* High availability with automatic failover in case a server, rack, or availability zone goes offline.
* Automatic data partitioning as data or load grows.

To interact with {{ ydb-short-name }}, you can use the [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md) and [SDK](../../../reference/ydb-sdk/index.md) fo C++, C#, Go, Java, Node.js, PHP, Python, and Rust.

{{ ydb-short-name }} supports a relational [data model](../../../concepts/datamodel/table.md) and manages [row oriented](../../datamodel/table.md#row-oriented-tables) and  [column oriented](../../datamodel/table.md#column-oriented-tables) tables with a predefined schema. To make it easier to organize tables, directories can be created like in the file system. In addition to tables, {{ ydb-short-name }} supports [topics](../../topic.md) as an entity for storing unstructured messages and delivering them to multiple subscribers.

Database commands are mainly written in YQL, an SQL dialect. This gives the user a powerful and already familiar way to interact with the database.

{{ ydb-short-name }} supports high-performance distributed [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) transactions that may affect multiple records of the same type in different tables. It provides the serializable isolation level, which is the strictest transaction isolation. You can also reduce the level of isolation to raise performance.

{{ ydb-short-name }} natively supports different processing options, such as [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing). The current version offers limited analytical query support. This is why we can say that {{ ydb-short-name }} is currently an OLTP database.

{{ ydb-short-name }} is an open-source system. The {{ ydb-short-name }} source code is available under [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Client applications interact with {{ ydb-short-name }} based on [gRPC](https://grpc.io/) that has an open specification. It allows implementing an SDK for any programming language.

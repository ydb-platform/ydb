# {{ ydb-short-name }} Overview

*{{ ydb-short-name }}* is a horizontally scalable, distributed, fault-tolerant DBMS. It is designed for high performance, with a typical server capable of handling tens of thousands of queries per second. The system is designed to handle hundreds of petabytes of data. {{ ydb-short-name }} can operate in both single data center and geo-distributed (cross data center) modes on a cluster of thousands of servers.

{{ ydb-short-name }} provides:

* [Strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_Consistency), which can be relaxed to increase performance.
* Support for queries written in [YQL](../../../yql/reference/index.md), an SQL dialect for working with big data.
* Automatic data replication.
* High availability with automatic failover if a server, rack, or availability zone goes offline.
* Automatic data partitioning as data or load grows.

To interact with {{ ydb-short-name }}, you can use the [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md) and [SDK](../../../reference/ydb-sdk/index.md) for C++, C#, Go, Java, Node.js, PHP, Python, and Rust.

{{ ydb-short-name }} supports a relational [data model](../../../concepts/datamodel/table.md) and manages [row-oriented](../../datamodel/table.md#row-oriented-tables) and [column-oriented](../../datamodel/table.md#column-oriented-tables) tables with a predefined schema. Directories can be created like in a file system to organize tables. In addition to tables, {{ ydb-short-name }} supports [topics](../../topic.md) for storing unstructured messages and delivering them to multiple subscribers.

Database commands are mainly written in YQL, an SQL dialect, providing a powerful and familiar way to interact with the database.

{{ ydb-short-name }} supports high-performance distributed [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) transactions that may affect multiple records in different tables. It provides the serializable isolation level, the strictest transaction isolation, with the option to reduce the isolation level to enhance performance.

{{ ydb-short-name }} natively supports different processing options, such as [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing). The current version offers limited analytical query support, which is why {{ ydb-short-name }} is currently considered an OLTP database.

{{ ydb-short-name }} is an open-source system. The source code is available under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Client applications interact with {{ ydb-short-name }} based on [gRPC](https://grpc.io/), which has an open specification, allowing for SDK implementation in any programming language.

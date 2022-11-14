---
title: YDB. DBMS overview
description: "Yandex Database (YDB): is a horizontally scalable distributed fault-tolerant DBMS. YDB is designed to meet high performance requirements. For example, a typical server can handle dozens of thousands of queries per second. The system is designed to handle hundreds of petabytes of data."
---
# {{ ydb-short-name }} overview

*{{ ydb-short-name }}* is a horizontally scalable distributed fault tolerant DBMS. {{ ydb-short-name }} is designed for high performance with a typical server being capable of handling tens of thousands of queries per second. The system is designed to handle hundreds of petabytes of data. {{ ydb-short-name }} can operate both in single datacenter and geodistributed (across several datacenters) modes on a cluster made up of thousands of servers.

{{ ydb-short-name }} provides:

* [Strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_Consistency), which can be lowered in order to raise performance.
* Support of [YQL](../../../yql/reference/index.md) queries (an SQL dialect for managing big data).
* Automatic data replication.
* High availability with automatic failover in case a server, rack, or availability zone goes offline.
* Automatic data partitioning as data or load grows.

To interact with {{ ydb-short-name }}, you can use the [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md) or [SDK](../../../reference/ydb-sdk/index.md) for {% if oss %}C++,{% endif %} Java, Python, Node.js, PHP, and Go.

{{ ydb-short-name }} supports a relational [data model](../../../concepts/datamodel/table.md) and manages tables with a predefined schema. To make it easier to organize tables, directories can be created like in the file system.

Database commands are mainly written in YQL, an SQL dialect. This gives the user a powerful and familiar way to interact with the database.

{{ ydb-short-name }} supports high-performance distributed [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) transactions that may affect multiple records in different tables. It provides the serializable isolation level, which is the strictest transaction isolation. You can also lower the level of isolation to raise performance.

{{ ydb-short-name }} natively supports different processing methods, such as [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing). The current version offers limited analytical query support. This is why we can say that {{ ydb-short-name }} is currently an OLTP database.

{{ ydb-short-name }} is used in Yandex services as a high-performance [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) DBMS. In particular, {{ yandex-cloud }} {{ objstorage-full-name}} use {{ ydb-short-name }} to store data and are based on its components.


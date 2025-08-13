# {{ ydb-short-name }} Architecture Overview

*{{ ydb-short-name }}* is a horizontally scalable, distributed, fault-tolerant DBMS. It is designed for high performance, with a typical server capable of handling tens of thousands of queries per second. The system is designed to handle hundreds of petabytes of data. {{ ydb-short-name }} can operate in both single data center and geo-distributed (cross data center) modes on a cluster of thousands of servers.

{{ ydb-short-name }} provides:

* [Strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_Consistency), which can be relaxed to increase performance.
* Support for queries written in [YQL](../yql/reference/index.md), an SQL dialect for working with big data.
* Automatic data replication.
* High availability with automatic failover if a server, rack, or availability zone goes offline.
* Automatic data partitioning as data or load grows.

To interact with {{ ydb-short-name }}, you can use the [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) and [SDK](../reference/ydb-sdk/index.md) for C++, C#, Go, Java, Node.js, PHP, Python, and Rust.

{{ ydb-short-name }} supports a relational [data model](/datamodel/table.md) and manages [row-oriented](datamodel/table.md#row-oriented-tables) and [column-oriented](datamodel/table.md#column-oriented-tables) tables with a predefined schema. Directories can be created like in a file system to organize tables. In addition to tables, {{ ydb-short-name }} supports [topics](topic.md) for storing unstructured messages and delivering them to multiple subscribers.

Database commands are mainly written in YQL, an SQL dialect, providing a powerful and familiar way to interact with the database.

{{ ydb-short-name }} supports high-performance distributed [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) transactions that may affect multiple records in different tables. It provides the serializable isolation level, the strictest transaction isolation, with the option to reduce the isolation level to enhance performance.

{{ ydb-short-name }} natively supports different processing options, such as [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing). The current version offers limited analytical query support, which is why {{ ydb-short-name }} is currently considered an OLTP database.

{{ ydb-short-name }} is an open-source system. The source code is available under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). Client applications interact with {{ ydb-short-name }} based on [gRPC](https://grpc.io/), which has an open specification, allowing for SDK implementation in any programming language.

## Use Cases {#use-cases}

{{ ydb-short-name }} can be used as an alternative solution in the following cases:

* When using NoSQL systems, if strong data consistency is required.
* When using NoSQL systems, if you need to make transactional updates to data stored in different rows of one or more tables.
* In systems that need to process and store large amounts of data and allow for virtually unlimited horizontal scalability (using industrial clusters of 5000+ nodes, processing millions of RPS, and storing petabytes of data).
* In low-load systems, when supporting a separate DB instance would be a waste of money (consider using {{ ydb-short-name }} in serverless mode instead).
* In systems with unpredictable or seasonally fluctuating load (you can add/reduce computing resources on request and/or in serverless mode).
* In high-load systems that shard load across relational DB instances.
* When developing a new product with no reliable load forecast or with an expected high load beyond the capabilities of conventional relational databases.
* In projects where the simultaneous handling of transactional and analytical workloads is required.

## How It Works?

Fully explaining how YDB works in detail takes quite a while. Below you can review several key highlights and then continue exploring the documentation to learn more.

### {{ ydb-short-name }} Architecture {#ydb-architecture}

![YDB architecture](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/grps.png)

{{ ydb-short-name }} clusters typically run on commodity hardware with a shared-nothing architecture. From a bird's eye view, {{ ydb-short-name }} exhibits a layered architecture. The compute and storage layers are disaggregated; they can either run on separate sets of nodes or be co-located.

One of the key building blocks of {{ ydb-short-name }}'s compute layer is called a *tablet*. Tablets are stateful logical components implementing various aspects of {{ ydb-short-name }}.

The next level of detail of the overall {{ ydb-short-name }} architecture is explained in the [{#T}](../contributor/general-schema.md) article.

### Hierarchy {#ydb-hierarchy}

![Hierarchy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/organization.png)

From the user's perspective, everything inside {{ ydb-short-name }} is organized in a hierarchical structure using directories. It can have arbitrary depth depending on how you choose to organize your data and projects. Even though {{ ydb-short-name }} does not have a fixed hierarchy depth like in other SQL implementations, it will still feel familiar as this is exactly how any virtual filesystem looks.

### Table {#table}

![Table](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/table.png)

{{ ydb-short-name }} provides users with a well-known abstraction — tables. In {{ ydb-short-name }}, there are two main types of tables:

* [Row-oriented tables](datamodel/table.md#row-tables) are designed for OLTP workloads.
* [Column-oriented tables](datamodel/table.md#column-tables) are designed for OLAP workloads.

Logically, from the user's perspective, both types of tables look the same. The main difference between row-oriented and column-oriented tables lies in how the data is physically stored. In row-oriented tables, the values of all columns in each row are stored together. In contrast, in column-oriented tables, each column is stored separately, meaning that cells from different rows are stored next to each other within the same column.

Regardless of the type, each table must have a primary key. Column-oriented tables can only have `NOT NULL` columns in primary keys. Table data is physically sorted by the primary key.

Partitioning works differently in row-oriented and column-oriented tables:

* Row-oriented tables are automatically partitioned by primary key ranges, depending on the data volume.
* Column-oriented tables are partitioned by the hash of the partitioning columns.

Each partition of a table is processed by a specific [tablet](glossary.md#tablets), called a [data shard](glossary.md#datashard) for row-oriented tables and a [column shard](glossary.md#columnshard) for column-oriented tables.

#### Split by Load {#split-by-load}

![Split by load](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/nagruz%201.5.png)

Data shards will automatically split into more as the load increases. They automatically merge back to the appropriate number when the peak load subsides.

#### Split by Size {#split-by-size}

![Split by size](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/size%201.5%20(1).png)

Data shards will also automatically split when the data size increases. They automatically merge back if enough data is deleted.

### Automatic Balancing {#automatic-balancing}

![Automatic balancing](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/pills%201.5.png)

{{ ydb-short-name }} evenly distributes tablets among available nodes. It moves heavily loaded tablets from overloaded nodes. CPU, memory, and network metrics are tracked to facilitate this.

### Distributed Storage Internals {#ds-internals}

![Distributed Storage internals](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/distributed.png)

{{ ydb-short-name }} doesn't rely on any third-party filesystem. It stores data by directly working with disk drives as block devices. All major disk kinds are supported: NVMe, SSD, or HDD. The PDisk component is responsible for working with a specific block device. The abstraction layer above PDisk is called VDisk. There is a special component called DSProxy between a tablet and VDisk. DSProxy analyzes disk availability and characteristics and chooses which disks will handle a request and which won't.

### Distributed Storage Proxy (DSProxy) {#ds-proxy}

![DSProxy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/proxy%202.png)

A common fault-tolerant setup of {{ ydb-short-name }} spans three datacenters or availability zones (AZ). When {{ ydb-short-name }} writes data to three AZs, it doesn't send requests to obviously bad disks and continues to operate without interruption even if one AZ and a disk in another AZ are lost.

## What's Next?

If you are interested in more specifics about various aspects of YDB, check out neighboring articles in this documentation section. If you are ready to jump into more practical content, you can continue to the [quick start](../quickstart.md) or [YQL](../dev/yql-tutorial/index.md) tutorials.
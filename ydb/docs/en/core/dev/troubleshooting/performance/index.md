# Troubleshooting performance issues

## Overview

Addressing database performance issues often requires a holistic approach, which includes optimizing queries, properly configuring hardware resources, and ensuring that both the database and the application are well-designed. Regular monitoring and maintenance are essential for proactively identifying and resolving these issues.

## Tools to troubleshoot performance issues

Troubleshooting performance issues in {{ ydb-short-name }} involves the following tools:

- [{{ ydb-short-name }} metrics](../../../reference/observability/metrics/index.md)

- [{{ ydb-short-name }} logs](../../../devops/manual/logging.md)

- [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md)


## Classification of {{ ydb-short-name }} performance issues

Database performance issues can be classified into several categories based on their nature:

- **Hardware infrastructure issues**.

    - **[Network issues](infrastructure/network.md)**. Insufficient bandwidth or network congestion in data centers can significantly affect {{ ydb-short-name }} performance.

    - **[Data center outages](infrastructure/dc-outage.md)**. Disruptions in the operations of data centers, which can lead to the unavailability of services or data. These outages can stem from various causes, including power failures, natural disasters, cyber attacks, etc. A common fault-tolerant setup of {{ ydb-short-name }} spans three data centers or availability zones (AZ). {{ ydb-short-name }} continues to operate without interruption even if one AZ is lost. However, {{ ydb-short-name }} will start the tablets from the offline AZ on the remaining online nodes. That process will result in temporarily higher query latencies. Distributed transactions that operate with tablets in the offline servers will suffer significant latencies.

    - **Data center drills**. Planned exercises conducted to prepare personnel for potential emergencies or outages. Depending on drill scenario, {{ ydb-short-name }} cluster may work in degraded mode until the drills are over.

- **[Hardware issues](infrastructure/hardware.md)**. Malfunctioning memory modules and network cards, until replaced, significantly impact database performance up to total unavailability of the affected server.

- **Insufficient resources**. These issues refer to situations when the workload demands more physical resources — such as CPU, memory, disk space, and network bandwidth — than allocated to a database.

    - **CPU bottlenecks**. High CPU usage can result in slow query processing and increased response times. When CPU resources are limited, the database may struggle to handle complex queries or large transaction loads.

    - **Insufficient disk space**. A lack of available disk space can prevent the database from storing new data, resulting in the database becoming read-only. This can also cause slowdowns as the system tries to reclaim disk space by compacting existing data more aggressively.

    - **Insufficient memory (RAM)**. If swap is disabled, insufficient memory can trigger [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory) that terminates the most memory-hungry processes (for servers running databases, it's often the database itself). If swap is enabled, insufficient memory can cause the database to rely heavily on disk I/O for operations, which is significantly slower than accessing data from memory. This can lead to increased latencies in query execution and data retrieval.

    - **Insufficient disk I/O bandwidth**. High read/write operations can overwhelm disk subsystems, leading to increased latencies in data access. When the system cannot read or write data quickly enough, queries that require disk access will be delayed.

- **OS and YDB-related issues**.

    - **[YDB updates](system/ydb-updates.md)**. YDB is a distributed system that supports rolling restart, when database administrators update YDB nodes one by one. This helps keep the YDB cluster up and running during the update process. However, when a YDB node is being restarted, Hive moves the tables that run on this node to other nodes, and that may lead to increased latencies for queries that are processed by the moving tables.

    - **Hardware resource allocation issues**. Suboptimal allocation of resources, for example poorly configured control groups (cgroups), may result in insufficient resources for {{ ydb-short-name }} and increase query latencies even though physical hardware resources are still available on the database server.

- **Schema design issues**. These issues stem from inefficient decisions made during the creation of tables and indices. They can significantly impact query performance.

- **Query-related issues**. These issues refer to database queries executing slower than expected because of their inefficient design.

## Most frequent issues that affect {{ ydb-short-name }} performance

It's recommended to troubleshoot YDB performance issues in the order of their occurrence frequency:

1. [Overloaded shards](schemas/overloaded-shards.md)
1. [Excessive tablet splits and merges](schemas/splits-merges.md)
1. [Frequent tablet transfers between nodes](system/tablets-moved.md)
1. Insufficient hardware resources:
    - Disk I/O bandwidth
    - Disk space
    - [Insufficient CPU](hardware/cpu-bottleneck.md)
1. [Hardware issues](infrastructure/hardware.md) and [data center outages](infrastructure/dc-outage.md)
1. [Network issues](infrastructure/network.md)
1. [{{ ydb-short-name }} updates](system/ydb-updates.md)
1. Time mismatch between nodes




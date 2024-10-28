# Troubleshooting performance issues

Addressing database performance issues often requires a holistic approach, which includes optimizing queries, properly configuring hardware resources, and ensuring that both the database and the application are well-designed. Regular monitoring and maintenance are essential for proactively identifying and resolving these issues.

## Tools to troubleshoot performance issues

Troubleshooting performance issues in {{ ydb-short-name }} involves the following tools:

- [{{ ydb-short-name }} metrics](../../../reference/observability/metrics/index.md)

- [{{ ydb-short-name }} logs](../../../devops/manual/logging.md)

- [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md)

- [Tracing](../../../reference/observability/tracing/setup.md)

- [Embedded UI](../../../reference/embedded-ui/index.md)

- [Query plans](../../query-plans-optimization.md)


## Classification of {{ ydb-short-name }} performance issues

Database performance issues can be classified into several categories based on their nature. This documentation section provides a high-level overview of these categories, starting with the lowest layers of the system and going all the way to the client. Below is a separate section for the [actual performance troubleshooting instructions](#instructions).

- **Hardware infrastructure issues**.

    - **[Network issues](infrastructure/network.md)**. Network congestion in data centers and especially between data centers can significantly affect {{ ydb-short-name }} performance.

    - **[Data center outages](infrastructure/dc-outage.md)**: Disruptions in data center operations that can cause service or data unavailability. These outages may result from various factors, such as power failures, natural disasters, or cyber-attacks. A common fault-tolerant setup for {{ ydb-short-name }} spans three data centers or availability zones (AZs). {{ ydb-short-name }} can continue operating without interruption, even if one data center and a server rack in another are lost. However, it will initiate the relocation of tablets from the offline AZ to the remaining online nodes, temporarily leading to higher query latencies. Distributed transactions involving tablets that are moving to other nodes might experience increased latencies.

    - **[Data center maintenance and drills](infrastructure/dc-drills.md)**. Planned maintenance or drills, exercises conducted to prepare personnel for potential emergencies or outages, can also affect query performance. Depending on the maintenance scope or drill scenario, some {{ ydb-short-name }} servers might become unavailable, which leads to the same impact as an outage.

    - **[Server hardware issues](infrastructure/hardware.md)**. Malfunctioning CPU, memory modules, and network cards, until replaced, significantly impact database performance up to the total unavailability of the affected server.

- **Insufficient resources**. These issues refer to situations when the workload demands more physical resources — such as CPU, memory, disk space, and network bandwidth — than allocated to a database. In some cases, suboptimal allocation of resources, for example poorly configured control groups (cgroups), may also result in insufficient resources for {{ ydb-short-name }} and increase query latencies even though physical hardware resources are still available on the database server.

    - **[CPU bottlenecks](hardware/cpu-bottleneck.md)**. High CPU usage can result in slow query processing and increased response times. When CPU resources are limited, the database may struggle to handle complex queries or large transaction loads.

    - **[Insufficient disk space](hardware/disk-space.md)**. A lack of available disk space can prevent the database from storing new data, resulting in the database becoming read-only. This can also cause slowdowns as the system tries to reclaim disk space by compacting existing data more aggressively.

    - **[Insufficient memory (RAM)](hardware/insufficient-memory.md)**. If swap is disabled, insufficient memory can trigger [OOM killer](https://en.wikipedia.org/wiki/Out_of_memory) that terminates the most memory-hungry processes (for servers running databases, it's often the database itself). If swap is enabled, insufficient memory can cause the database to rely heavily on disk I/O for operations, which is significantly slower than accessing data from memory. This can lead to increased latencies in query execution and data retrieval.

    - **[Insufficient disk I/O bandwidth](hardware/io-bandwidth.md)**. High read/write operations can overwhelm disk subsystems, leading to increased latencies in data access. When the system cannot read or write data quickly enough, queries that require disk access will be delayed.

- **OS issues**


    - **[System clock drift](system/system-clock-drift.md)**. If the system clocks on the {{ ydb-short-name }} servers start to drift apart, it will lead to increased distributed transaction latencies. In severe cases, {{ ydb-short-name }} might even refuse to process distributed transactions and return errors.

    - Other processes running on the same servers or virtual machines as {{ ydb-short-name }}, such as antiviruses, observability agents, etc.

    - Kernel misconfiguration.

- **{{ ydb-short-name }}-related issues**

    - **[Rolling restart](ydb/ydb-updates.md)**. Database administrators (DBAs) can keep the {{ ydb-short-name }} cluster up and running during the update process or some {{ ydb-short-name }} configuration changes. This is possible because {{ ydb-short-name }} is a distributed system that supports rolling restart, and DBAs can update {{ ydb-short-name }} nodes one by one. However, when a {{ ydb-short-name }} node is being restarted, [Hive](../../../concepts/glossary.md#hive) moves the tablets that run on this node to other nodes, and that may lead to increased latencies for queries that are processed by the moving tablets.

    - Actor system pools misconfiguration.

    - SDK usage issues.

- **Schema design issues**. These issues stem from inefficient decisions made during the creation of tables and indices. They can significantly impact query performance.

- **Client application issues**. These issues refer to database queries executing slower than expected because of their inefficient design.

## Instructions {#instructions}

To troubleshoot {{ ydb-short-name }} performance issues, treat each potential cause as a hypothesis. Systematically review the list of hypotheses and verify whether they apply to your situation. The documentation for each cause provides a description, guidance on how to check diagnostics, and recommendations on what to do if the hypothesis is confirmed.

If any known changes occurred in the system around the time the performance issues first appeared, investigate those first. Otherwise, follow this recommended order for evaluating potential root causes. This order is loosely based on the descending frequency of their occurrence on large production {{ ydb-short-name }} clusters.

1. [Overloaded shards](schemas/overloaded-shards.md)
1. [Excessive tablet splits and merges](schemas/splits-merges.md)
1. [Frequent tablet moves between nodes](ydb/tablets-moved.md)
1. Insufficient hardware resources:
    - [Disk I/O bandwidth](hardware/io-bandwidth.md)
    - [Disk space](hardware/disk-space.md)
    - [Insufficient CPU](hardware/cpu-bottleneck.md)
1. [Hardware issues](infrastructure/hardware.md) and [data center outages](infrastructure/dc-outage.md)
1. [Network issues](infrastructure/network.md)
1. [{{ ydb-short-name }} updates](ydb/ydb-updates.md)
1. [System clock drift](system/system-clock-drift.md)


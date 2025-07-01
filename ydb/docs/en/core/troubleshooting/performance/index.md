# Troubleshooting performance issues

Addressing database performance issues often requires a holistic approach, which includes optimizing queries, properly configuring hardware resources, and ensuring that both the database and the application are well-designed. Regular monitoring and maintenance are essential for proactively identifying and resolving these issues.

## Tools to troubleshoot performance issues

Troubleshooting performance issues in {{ ydb-short-name }} involves the following tools:

- [{{ ydb-short-name }} metrics](../../reference/observability/metrics/index.md)

    Diagnistic steps for most performance issues involve analyzing [Grafana dashboards](../../reference/observability/metrics/grafana-dashboards.md) that use {{ ydb-short-name }} metrics collected by Prometheus For information about how to set up Grafana and Prometheus, see [{#T}](../../devops/observability/monitoring.md).

- [{{ ydb-short-name }} logs](../../devops/observability/logging.md)
- [Tracing](../../reference/observability/tracing/setup.md)
- [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md)
- [Embedded UI](../../reference/embedded-ui/index.md)
- [Query plans](../../dev/query-plans-optimization.md)
- Third-party observability tools

## Classification of {{ ydb-short-name }} performance issues

Database performance issues can be classified into several categories based on their nature. This documentation section provides a high-level overview of these categories, starting with the lowest layers of the system and going all the way to the client. Below is a separate section for the [actual performance troubleshooting instructions](#instructions).

### Hardware infrastructure issues

- **[Network issues](infrastructure/network.md)**. Network congestion in data centers and especially between data centers can significantly affect {{ ydb-short-name }} performance.

- **[Data center outages](infrastructure/dc-outage.md)**: Disruptions in data center operations that can cause service or data unavailability. To address this concern, {{ ydb-short-name }} cluster can be configured to span three data centers or availability zones, but the performance aspect needs to be taken into account too.

- **[Data center maintenance and drills](infrastructure/dc-drills.md)**. Planned maintenance or drills, exercises conducted to prepare personnel for potential emergencies or outages, can also affect query performance. Depending on the maintenance scope or drill scenario, some {{ ydb-short-name }} servers might become unavailable, which leads to the same impact as an outage.

- **[Server hardware issues](infrastructure/hardware.md)**. Malfunctioning CPU, memory modules, and network cards, until replaced, significantly impact database performance or lead to the unavailability of the affected server.

### Insufficient resource issues

These issues refer to situations when the workload demands more physical resources — such as CPU, memory, disk space, and network bandwidth — than allocated to a database. In some cases, suboptimal allocation of resources, for example misconfigured [control groups (cgroups)](https://en.wikipedia.org/wiki/Cgroups) or [actor system pools](../../concepts/glossary.md#actor-system-pool), may also result in insufficient resources for {{ ydb-short-name }} and increase query latencies even though physical hardware resources are still available on the database server.

- **[CPU bottlenecks](hardware/cpu-bottleneck.md)**. High CPU usage can result in slow query processing and increased response times. When CPU resources are limited, the database may struggle to handle complex queries or large transaction loads.

- **[Insufficient disk space](hardware/disk-space.md)**. A lack of available disk space can prevent the database from storing new data, resulting in the database becoming read-only. This might also cause slowdowns as the system tries to reclaim disk space by compacting existing data more aggressively.

- **[Insufficient memory (RAM)](hardware/insufficient-memory.md)**. Queries require memory to temporarily store various intermediate data during execution. A lack of available memory can negatively impact database performance in multiple ways.

- **[Insufficient disk I/O bandwidth](hardware/io-bandwidth.md)**. A high rate of read/write operations can overwhelm the disk subsystem, causing increased data access latencies. When the [distributed storage](../../concepts/glossary.md#distributed-storage) cannot read or write data quickly enough, queries requiring disk access will take longer to execute.

### Operating system issues

- **[System clock drift](system/system-clock-drift.md)**. If the system clocks on the {{ ydb-short-name }} servers start to drift apart, it will lead to increased distributed transaction latencies. In severe cases, {{ ydb-short-name }} might even refuse to process distributed transactions and return errors.

- Other processes running on the same servers or virtual machines as {{ ydb-short-name }}, such as antiviruses, observability agents, etc.

- Kernel misconfiguration.

### {{ ydb-short-name }}-related issues

- **[Updating {{ ydb-short-name }} versions](ydb/ydb-updates.md)**. There are two main related aspects: restarting all nodes within a relatively short timeframe, and the behavioral differences between versions.

- Actor system pools misconfiguration.

### Schema design issues

- **[{#T}](./schemas/overloaded-shards.md)**. Data shards serving row-oriented tables may become overloaded for several reasons. Such overload leads to increased latencies for the transactions processed by the affected data shards.

- **[{#T}](./schemas/splits-merges.md)**. {{ ydb-short-name }} supports automatic splitting and merging of data shards, which allows it to seamlessly adapt to changes in workloads. However, these operations are not free and might have a short-term negative impact on query latencies.

### Client application-related issues

- **Query design issues**. Inefficiently designed database queries may execute slower than expected.

- **SDK usage issues**. Issues related to improper or suboptimal use of the SDK.

## Instructions {#instructions}

To troubleshoot {{ ydb-short-name }} performance issues, treat each potential cause as a hypothesis. Systematically review the list of hypotheses and verify whether they apply to your situation. The documentation for each cause provides a description, guidance on how to check diagnostics, and recommendations on what to do if the hypothesis is confirmed.

If any known changes occurred in the system around the time the performance issues first appeared, investigate those first. Otherwise, follow this recommended order for evaluating potential root causes. This order is loosely based on the descending frequency of their occurrence on large production {{ ydb-short-name }} clusters.

1. Overloaded [shards](schemas/overloaded-shards.md) and [errors](./queries/overloaded-errors.md)
1. [Excessive tablet splits and merges](schemas/splits-merges.md)
1. [Frequent tablet moves between nodes](ydb/tablets-moved.md)
1. Insufficient hardware resources:

    - [Disk I/O bandwidth](hardware/io-bandwidth.md)
    - [Disk space](hardware/disk-space.md)
    - [Insufficient CPU](hardware/cpu-bottleneck.md)
    - [Insufficient memory](hardware/insufficient-memory.md)

1. [Hardware issues](infrastructure/hardware.md) and [data center outages](infrastructure/dc-outage.md)
1. [Network issues](infrastructure/network.md)
1. [{#T}](ydb/ydb-updates.md)
1. [System clock drift](system/system-clock-drift.md)
1. [{#T}](queries/transaction-lock-invalidation.md)
1. [{#T}](infrastructure/dc-drills.md)

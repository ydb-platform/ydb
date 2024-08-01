# {{ ydb-short-name }} cluster topology

A {{ ydb-short-name }} cluster consists of [storage](./glossary.md#storage-node) and [database](./glossary.md#database-node) nodes. As the data stored in {{ ydb-short-name }} is available only via queries and API calls, both types of nodes are essential for [database availability](#database-availability). However, [distributed storage](glossary.md#distributed-storage) consisting of storage nodes has the most impact on the cluster's fault tolerance and ability to persist data reliably. During the initial cluster deployment, an appropriate distributed storage [operating mode](#cluster-config) needs to be chosen according to the expected workload and [fault tolerance](#fault-tolerance) requirements.

## Cluster operating modes {#cluster-config}

Cluster topology is based on the chosen operating mode, which needs to be determined according to the fault tolerance requirements. {{ ydb-short-name }}'s failure model is based on the concepts of [fail domain](./glossary.md#fail-domain) and [fail realm](./glossary.md#fail-realm).

The following {{ ydb-short-name }} distributed storage operating modes are available:

- `none`. There is no redundancy. Any hardware failure causes data to become unavailable or permanently lost. This mode is only recommended for development and functional testing.
- `block-4-2`. [Erasure coding](https://en.wikipedia.org/wiki/Erasure_code) is applied with two blocks of redundancy added to the four blocks of source data. Storage nodes are placed in at least 8 failure domains (usually racks). A [storage pool](./glossary.md#storage-pool) remains available if any two domains fail, continuing to record all 6 data parts in the remaining domains. This mode is recommended for clusters deployed within a single availability zone or data center.
- `mirror-3-dc`. Data is replicated to 3 availability zones using 3 failure domains (usually racks) within each zone. {{ ydb-short-name }} cluster remains available even if one availability zone completely fails; additionally, one failure domain in the remaining zones can fail at the same time. This mode is recommended for multi-datacenter clusters with high availability requirements.
- `mirror-3-dc-3-nodes`. A simplified version of `mirror-3-dc`. This mode requires at least 3 servers with 3 disks each. Each server must be located in an independent data center to provide reasonable fault tolerance. System health in this mode is maintained if no more than one server fails. This mode is only recommended for functional testing or building prototypes.

{% note info %}

Node failure means both its total and partial unavailability, for example, failure of a single disk on a node.

{% endnote %}

The table below describes the requirements and fault tolerance levels for different operating modes:

| Mode | Storage<br>volume multiplier | Minimum<br>number<br>of nodes | Fail<br>domain | Fail<br>realm | Number of<br>data centers | Number of<br>server racks |
| --- | --- | --- | --- | --- | --- | --- |
| `none`, no fault tolerance | 1 | 1 | Node | Node | 1 | 1 |
| `block-4-2`, can stand a failure of 2 racks | 1.5 | 8 (10 recommended) | Rack | Data center | 1 | 8 |
| `mirror-3-dc`, can stand a failure of a data center and 1 rack in one of the remaining data centers | 3 | 9 (12 recommended) | Rack | Data center | 3 | 3 in each data center |
| `block-4-2` *(reduced)*, can stand a failure of 1 rack | 1.5 | 10 | ½ a rack | Data center | 1 | 5 |
| `mirror-3-dc` *(reduced)*, can stand a failure of a data center and 1 server in one of the two other data centers | 3 | 12 | ½ a rack | Data center | 3 | 6 |
| `mirror-3-dc` *(3 nodes)*, can stand a failure of a single server, or a failure of a data center | 3 | 3 | Server | Data center | 3 | Doesn't matter |

{% note info %}

The storage volume multiplier specified above only applies to the fault tolerance factor. Other influencing factors (for example, [slot](./glossary.md#slot) fragmentation and granularity) must be taken into account for storage size planning.

{% endnote %}

When creating a [storage group](glossary.md#storage-group), which is a basic allocation unit for storage management, {{ ydb-short-name }} selects [VDisks](./glossary.md#vdisk) that are located on [PDisks](./glossary.md#pdisk) from different fail domains. For `block-4-2` mode, a storage group should be distributed across at least 8 fail domains, while for `mirror-3-dc` mode, it should be distributed across 3 fail realms, with at least 3 fail domains in each realm.

For information about how to set the {{ ydb-short-name }} cluster topology, see [{#T}](../deploy/configuration/config.md#domains-blob).

### Reduced configurations {#reduced}

If it is impossible to use the [recommended amount](#cluster-config) of hardware, you can divide servers within a single rack into two dummy fail domains. In this configuration, the failure of one rack results in the failure of two domains instead of just one. In such reduced configurations, {{ ydb-short-name }} will continue to operate if two domains fail. The minimum number of racks in a cluster is five for `block-4-2` mode and two per data center (e.g., six in total) for `mirror-3-dc` mode.

The minimal fault-tolerant configuration of a {{ ydb-short-name }} cluster uses the `mirror-3-dc-3-nodes` operating mode, which requires only three servers. In this configuration, each server acts as both a fail domain and a fail realm, and the cluster can withstand the failure of only a single server.

## Fault tolerance {#fault-tolerance}

If a disk fails, {{ ydb-short-name }} can automatically reconfigure a storage group. Whether the disk failure is caused by the whole server failure or not is irrelevant in this context. Such reconfiguration replaces the VDisk located on the failed hardware with a new VDisk, and the system tries to place it on operational hardware. The same rules apply as when creating a group:

* The new VDisk is created in a fail domain different from any other VDisks in the group.
* In the `mirror-3-dc` mode, it is created within the same fail realm as the failed VDisk.

This process can cause issues when a cluster's hardware is distributed across the minimum required number of fail domains:

* If an entire fail domain is down, reconfiguration becomes impractical, as a new VDisk can only be placed in the fail domain that is down.
* Reconfiguration is possible only if part of a fail domain is down. However, the load previously handled by the failed hardware will be redistributed across the hardware, remaining in the same fail domain.

The load can be redistributed across all the hardware that is still running if the number of fail domains in a cluster exceeds the minimum amount required for creating storage groups by at least one. This means having 9 domains for `block-4-2` and 4 domains in each fail realm for `mirror-3-dc`, which is recommended.

## Capacity and performance considerations

The system can work with fail domains of any size. However, if there are few domains and different numbers of disks in different domains, the number of storage groups that can be created will be limited. In this case, some hardware in fail domains that are too large may be underutilized. If all hardware is used fully, significant differences in domain sizes may make reconfiguration impossible.

For example, consider a cluster in `block-4-2` mode with 15 racks. The first of the 15 racks hosts 20 servers, while the other 14 racks host 10 servers each. To fully utilize all 20 servers from the first rack, {{ ydb-short-name }} will create groups so that 1 disk from this largest fail domain is used in each group. As a result, if the hardware in any other fail domain fails, the load cannot be redistributed to the hardware in the first rack.

{{ ydb-short-name }} can group disk drives of different vendors, capacities, and speeds. The resulting characteristics of a group depend on the set of the worst characteristics of the hardware serving the group. Generally, the best results can be achieved by using homogeneous hardware.

{% note info %}

Hardware from the same batch is more likely to have similar defects and may fail simultaneously. It is essential to consider this when building large-scale {{ ydb-short-name }} clusters.

{% endnote %}

Therefore, the optimal initial hardware configurations for production {{ ydb-short-name }} clusters are as follows:

* **A cluster hosted in one availability zone**: This setup uses the `block-4-2` mode and consists of nine or more racks, each with an identical number of servers.
* **A cluster hosted in three availability zones**: This setup uses the `mirror-3-dc` mode and is distributed across three data centers, with four or more racks in each, all containing an identical number of servers.

## Redundancy recovery {#rebuild}

Auto reconfiguration of storage groups reduces the risk of data loss in the event of a sequence of failures, provided these failures occur with sufficient time intervals to recover redundancy. By default, reconfiguration begins one hour after {{ ydb-short-name }} detects a failure.

Once a group is reconfigured, a new VDisk is automatically populated with data to restore the required storage redundancy. This process increases the load on other VDisks in the group as well as on the network. The total data replication speed is limited on both the source and target VDisks to minimize the impact of redundancy recovery on system performance.

The time required to restore redundancy depends on the amount of data and hardware performance. For example, replication on fast NVMe SSDs may take an hour, while it could take more than 24 hours on large HDDs. To ensure reconfiguration is possible, a cluster should have free slots available for creating VDisks in different fail domains. When determining the number of slots to keep free, consider the risk of hardware failure, the time required to replicate data, and the time needed to replace failed hardware.

## Database availability {#database-availability}

A [database](./glossary.md#database) within a {{ ydb-short-name }} cluster is available if both its storage and compute resources are operational:

- All [storage groups](./glossary.md#storage-group) allocated for the database must be operational, i.e., stay within the allowed level of failures.
- The compute resources of the currently available [database nodes](./glossary.md#database-node) (primarily the amount of main memory) must be sufficient to start all the [tablets](glossary.md#tablet) managing objects like [tables](glossary.md#table) or [topics](glossary.md#topic) within the database and to handle user sessions.

To survive an entire data center outage at the database level, assuming a cluster configured with the `mirror-3-dc` operating mode:

- The [storage nodes](./glossary.md#storage-node) need to have at least double the I/O bandwidth and disk capacity compared to what is required for normal operation. In the worst case, the load on the remaining nodes during the maximum allowed outage might triple, but that's only temporary until self-heal restores failed disks in operating data centers.
- The [database nodes](./glossary.md#database-node) must be evenly distributed between all 3 data centers and include sufficient resources to handle the entire workload when running in just 2 of the 3 data centers. To achieve this, database nodes in each datacenter need at least 35% extra spare CPU and RAM resources when running normally without ongoing failures. If database nodes are typically utilized above this threshold, consider adding more of them or moving them to servers with more resources.

## See also

* [Documentation for DevOps Engineers](../devops/index.md)
* [Example cluster configuration files](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/)

# Topology

A {{ ydb-short-name }} cluster consists of static and dynamic nodes.

* Static nodes enable data storage, implementing one of the supported redundancy schemes depending on the established operating mode.
* Dynamic nodes enable query execution, transaction coordination, and other data control functionality.

Cluster topology is determined by the fault tolerance requirements. The following operating modes are available:

| Mode | Storage<br>volume multiplier | Minimum<br>number<br>of nodes | Description |
--- | --- | --- | ---
| `none` | 1 | 1 | There is no redundancy.<br>Any hardware failure causes the storage pool to become unavailable.<br>This mode is only recommended for functional testing. |
| `block-4-2` | 1.5 | 8 | [Erasure coding](https://en.wikipedia.org/wiki/Erasure_code) with two blocks of redundancy added to the four blocks of source data is applied. Storage nodes are placed in at least 8 failure domains (usually racks).<br>The storage pool is available if any two domains fail, continuing to record all 6 data parts in the remaining domains.<br>This mode is recommended for storage pools within a single availability zone (usually a data processing center). |
| `mirror-3-dc` | 3 | 9 | Data is replicated to 3 availability zones using 3 failure domains (usually racks) within each zone.<br>The storage pool is available if one availability zone and one failure domain fail in the remaining zones.<br>This mode is recommended for multi-data center installations. |
| `mirror-3dc-3-nodes` | 3 | 3 | It is a simplified version of `mirror-3dc`. This mode requires at least 3 servers with 3 disks each. Each server must be located in an independent data center in order to provide the best fault tolerance.<br>Health in this mode is maintained if no more than 1 node fails.<br>This mode is only recommended for functional testing. |

{% note info %}

Node failure means both its total and partial unavailability, for example, failure of a single disk on a node.

The storage volume multiplier specified above only applies to the fault tolerance factor. Other influencing factors (for example, slot fragmentation and granularity) must be taken into account for storage size planning.

{% endnote %}

For information about how to set the {{ ydb-short-name }} cluster topology, see [{#T}](../deploy/configuration/config.md#domains-blob).

## Production configurations of distributed storage

To ensure the fault tolerance of a {{ ydb-short-name }} cluster, properly configure the [distributed storage](glossary.md#distributed-storage). Select the appropriate [fault tolerance mode](#fault-tolerance) and [cluster configuration](#cluster-config) for your cluster according to the workload requirements.

### Fault tolerance modes {#fault-tolerance}

We recommend using the following [fault tolerance modes](topology.md) for {{ ydb-short-name }} production installations:

* `block-4-2`: For a cluster hosted in a single availability zone.
* `mirror-3-dc`: For a cluster hosted in three availability zones.

A fail model of {{ ydb-short-name }} is based on concepts such as fail domain and fail realm.

Fail domain

: A set of hardware that may fail concurrently.

  For example, a fail domain includes disks of the same server (as all server disks may be unavailable if the server PSU or network controller is down). A fail domain also includes servers located in the same server rack (as the entire hardware in the rack may be unavailable if there is a power outage or some issue with the network hardware in the same rack).

  Any domain fail is handled automatically, without shutting down the system.

Fail realm

: A set of fail domains that may fail concurrently.

  An example of a fail realm is hardware located in the same data center that may fail as a result of a natural disaster.

Usually a fail domain is a server rack, while a fail realm is a data center.

When creating a [storage group](glossary.md#storage-groups), {{ ydb-short-name }} chooses VDisks that are located on PDisks from different fail domains. For `block-4-2` mode, a storage group should be distributed across at least 8 fail domains, and for `mirror-3-dc` mode, across 3 fail realms, with at least 3 fail domains in each realm.

### Cluster configuration {#cluster-config}

If a disk fails, {{ ydb-short-name }} may automatically reconfigure a storage group. In this reconfiguration, a new VDisk replaces the VDisk located on the failed hardware, and the system tries to place it on operational hardware during the reconfiguration process. The same rules apply as when creating a group: the new VDisk is created in a fail domain different from any other VDisks in the group (and within the same fail realm as the failed VDisk in the `mirror-3-dc` mode).

This setup can cause issues when a cluster's hardware is distributed across the minimum required number of fail domains:

* If an entire fail domain is down, reconfiguration becomes impractical, as a new VDisk can only be placed in the fail domain that is down.
* Reconfiguration is possible only if part of a fail domain is down. However, the load previously handled by the failed hardware will be redistributed across the hardware, remaining in the same fail domain.

If the number of fail domains in a cluster exceeds the minimum amount required for creating storage groups at least by one (that is, 9 domains for `block-4-2` and 4 domains in each fail realm for `mirror-3-dc)`, in case some hardware fails, the load can be redistributed across all the hardware that is still running.

The system can work with fail domains of any size. However, if there are few domains and a different number of disks in different domains, the number of storage groups that you can create will be limited. In this case, some hardware in fail domains that are too large may be underutilized. If the hardware is used in full, significant distortions in domain sizes may make reconfiguration impossible.

For example, there are 15 racks in a cluster with `block-4-2` fault tolerance mode. The first of the 15 racks hosts 20 servers and the other 14 racks host 10 servers each. To fully utilize all the 20 servers from the first rack, {{ ydb-short-name }} will create groups so that 1 disk from this largest fail domain is used in each group. As a result, if any other fail domain's hardware is down, the load can't be distributed to the hardware in the first rack.

{{ ydb-short-name }} can group disk drives of different vendors, capacities, and speeds. The resulting characteristics of a group depend on the set of the worst characteristics of the hardware serving the group. Generally, the best results can be achieved by using homogenous hardware. 

{% note info %}

Hardware from the same batch is more likely to have similar defects and may fail simultaneously. It is essential to consider this when building large-scale {{ ydb-short-name }} clusters.

{% endnote %}

Therefore, the optimal initial hardware configurations for production {{ ydb-short-name }} clusters look like this:

* **A cluster hosted in one availability zone**: This setup uses the `block-4-2` fault tolerance mode and consists of nine or more racks, each with an identical number of servers.
* **A cluster hosted in three availability zones**: This setup uses the `mirror-3-dc` fault tolerance mode and is distributed across three data centers, with four or more racks in each and an identical number of servers.

See also [{#T}](#reduced).

### Redundancy recovery {#rebuild}

Auto reconfiguration of storage groups reduces the risk of data loss in the event of a sequence of failures, provided these failures occur with sufficient time intervals to recover redundancy. By default, reconfiguration begins one hour after {{ ydb-short-name }} detects a failure.

Once a group is reconfigured, a new VDisk is automatically populated with data to restore the required storage redundancy. This process increases the load on other VDisks in the group as well as on the network. The total data replication speed is limited on both the source and target VDisks to minimize the impact of redundancy recovery on system performance.

The time required to restore redundancy depends on the amount of data and hardware performance. For example, replication on fast NVMe SSDs may take an hour, while it could take more than 24 hours on large HDDs. To ensure reconfiguration is possible, a cluster should have free slots available for creating VDisks in different fail domains. When determining the number of slots to keep free, consider the risk of hardware failure, the time required to replicate data, and the time needed to replace failed hardware.

### Simplified hardware configurations {#reduced}

If it is impossible to use the [recommended amount](#cluster-config) of hardware, you can divide servers within a single rack into two dummy fail domains. In this configuration, the failure of one rack results in the failure of two domains instead of just one. In [both fault tolerance modes](#fault-tolerance), {{ ydb-short-name }} will continue to operate if two domains fail. When using the configuration with dummy fail domains, the minimum number of racks in a cluster is five for `block-4-2` mode and two per data center for `mirror-3-dc` mode.

### Fault tolerance level {#reliability}

The table below describes fault tolerance levels for different fault tolerance modes and hardware configurations of a {{ ydb-short-name }} cluster:

Fault tolerance<br>mode | Fail<br>domain | Fail<br>realm | Number of<br>data centers | Number of<br>server racks | Fault tolerance<br>level
:--- | :---: | :---: | :---: | :---: | :---
`block-4-2` | Rack | Data center | 1 | 9 or more | Can stand a failure of 2 racks
`block-4-2` | ½ a rack | Data center | 1 | 5 or more | Can stand a failure of 1 rack
`block-4-2` | Server | Data center | 1 | Doesn't matter | Can stand a failure of 2 servers
`mirror-3-dc` | Rack | Data center | 3 | 4 in each data center | Can stand a failure of a data center and 1 rack in one of the two other data centers
`mirror-3-dc` | Server | Data center | 3 | Doesn't matter | Can stand a failure of a data center and 1 server in one of the two other data centers

## See also

* [Documentation for DevOps Engineers](../devops/index.md)
* [Example configurations](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/)

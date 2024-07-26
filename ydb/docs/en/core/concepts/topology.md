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

## Production configurations of data storage

To ensure the required fault tolerance of {{ ydb-short-name }}, configure the [cluster disk subsystem](cluster/distributed_storage.md) properly: select the appropriate [fault tolerance mode](#fault-tolerance) and [cluster configuration](#cluster-config) for your cluster.

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

When creating a [storage group](glossary.md#storage-groups), {{ ydb-short-name }} groups VDisks that are located on PDisks from different fail domains. For `block-4-2` mode, a PDisk should be distributed across at least 8 fail domains, and for `mirror-3-dc` mode, across 3 fail realms with at least 3 fail domains in each of them.

### Cluster configuration {#cluster-config}

If a disk fails, {{ ydb-short-name }} may automatically reconfigure a storage group so that, instead of the VDisk located on the failed hardware, a new VDisk is used that the system tries to place on hardware that is running while the group is being reconfigured. In this case, the same rule applies as when creating a group: a VDisk is created in a fail domain that is different from the fail domains of any other VDisk in this group (and in the same fail realm as that of the failed VDisk for `mirror-3-dc`).

This causes some issues when a cluster's hardware is distributed across the minimum required amount of fail domains:

* If the entire fail domain is down, reconfiguration no longer makes sense, since a new VDisk can only be located in the fail domain that is down.
* If part of a fail domain is down, reconfiguration is possible, but the load that was previously handled by the failed hardware will only be redistributed across hardware in the same fail domain.

If the number of fail domains in a cluster exceeds the minimum amount required for creating storage groups at least by one (that is, 9 domains for `block-4-2` and 4 domains in each fail realm for `mirror-3-dc)`, in case some hardware fails, the load can be redistributed across all the hardware that is still running.

The system can work with fail domains of any size. However, if there are few domains and a different number of disks in different domains, the number of storage groups that you can create will be limited. In this case, some hardware in fail domains that are too large may be underutilized. If the hardware is used in full, significant distortions in domain sizes may make reconfiguration impossible.

For example, there are 15 racks in a cluster with `block-4-2` fault tolerance mode. The first of the 15 racks hosts 20 servers and the other 14 racks host 10 servers each. To fully utilize all the 20 servers from the first rack, {{ ydb-short-name }} will create groups so that 1 disk from this largest fail domain is used in each group. As a result, if any other fail domain's hardware is down, the load can't be distributed to the hardware in the first rack.

{{ ydb-short-name }} can group disks of different vendors, capacity, and speed. The resulting characteristics of a group depend on a set of the worst characteristics of the hardware that is serving the group. Usually the best results can be achieved if you use same-type hardware. When creating large clusters, keep in mind that hardware from the same batch is more likely to have the same defect and fail simultaneously.

Therefore, we recommend the following optimal hardware configurations for production installations:

* **A cluster hosted in 1 availability zone**: It uses `block4-2` fault tolerance mode and consists of 9 or more racks with the same amount of identical servers in each rack.
* **A cluster hosted in 3 availability zones**: It uses `mirror3-dc` fault tolerance mode and is distributed across 3 data centers with 4 or more racks in each of them, the racks being equipped with the same amount of identical servers.

See also [{#T}](#reduced).

### Redundancy recovery {#rebuild}

Auto reconfiguration of storage groups reduces the risk of data loss in the event of multiple failures that occur within intervals sufficient to recover the redundancy. By default, reconfiguration is done one hour after {{ ydb-short-name }} detects a failure.

Once a group is reconfigured, a new VDisk is automatically populated with data to restore the required storage redundancy in the group. This increases the load on other VDisks in the group and the network. To reduce the impact of redundancy recovery on the system performance, the total data replication speed is limited both on the source and target VDisks.

The time it takes to restore the redundancy depends on the amount of data and hardware performance. For example, replication on fast NVMe SSDs may take an hour, while on large HDDs more than 24 hours. To make reconfiguration possible in general, a cluster should have free slots for creating VDisks in different fail domains. When determining the number of slots to be kept free, factor in the risk of hardware failure, the time it takes to replicate data and replace the failed hardware.

### Simplified hardware configurations {#reduced}

If it's not possible to use the [recommended amount](#cluster-config) of hardware, you can divide servers within a single rack into two dummy fail domains. In this configuration, a failure of 1 rack means a failure of 2 domains and not a single one. In [both fault tolerance modes](#fault-tolerance), {{ ydb-short-name }} will keep running if 2 domains fail. If you use the configuration with dummy fail domains, the minimum number of racks in a cluster is 5 for `block-4-2` mode and 2 in each data center for `mirror-3-dc` mode.

### Fault tolerance level {#reliability}

The table below describes fault tolerance levels for different fault tolerance modes and hardware configurations of a {{ ydb-short-name }} cluster:

Fault tolerance<br>mode | Fail<br>domain | Fail<br>realm | Number of<br>data centers | Number of<br>server racks | Fault tolerance<br>level
:--- | :---: | :---: | :---: | :---: | :---
`block-4-2` | Rack | Data center | 1 | 9 or more | Can stand a failure of 2 racks
`block-4-2` | ½ a rack | Data center | 1 | 5 or more | Can stand a failure of 1 rack
`block-4-2` | Server | Data center | 1 | Doesn't matter | Can stand a failure of 2 servers
`mirror-3-dc` | Rack | Data center | 3 | 4 in each data center | Can stand a failure of a data center and 1 rack in one of the two other data centers
`mirror-3-dc` | Server | Data center | 3 | Doesn't matter | Can stand a failure of a data center and 1 server in one of the two other data centers

## Node Broker Configuration {#node-broker-config}

Node Broker is a system tablet that registers dynamic nodes in the {{ ydb-short-name }} cluster.

Node broker gives names to dynamic nodes when they register. By default, a node name consists of the hostname and the port on which the node is running.

In a dynamic environment where hostnames often change, such as in Kubernetes, using hostname and port leads to an uncontrollable increase in the number of unique node names. This is true even for a database with a handful of dynamic nodes. Such behavior may be undesirable for a time series monitoring system as the number of metrics grows uncontrollably. To solve this problem, the system administrator can set up *stable* node names.

A stable name identifies a node within the tenant. It consists of a prefix and a node's sequential number within its tenant. If a dynamic node has been shut down, after a timeout, its stable name can be taken by a new dynamic node serving the same tenant.

To enable stable node names, you need to add the following to the cluster configuration:

```yaml
feature_flags:
  enable_stable_node_names: true
```

By default, the prefix is `slot-`. To override the prefix, add the following to the cluster configuration:

```yaml
node_broker_config:
  stable_node_name_prefix: <new prefix>
```

## Sample cluster configurations {#examples}

You can find model cluster configurations for deployment in the [repository](https://github.com/ydb-platform/ydb/tree/main/ydb/deploy/yaml_config_examples/). Check them out before deploying a cluster.

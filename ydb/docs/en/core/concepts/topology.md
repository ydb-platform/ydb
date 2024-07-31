# Topology

A {{ ydb-short-name }} cluster consists of static and dynamic nodes.

* Static nodes enable data storage, implementing one of the supported redundancy schemes depending on the established operating mode.
* Dynamic nodes enable query execution, transaction coordination, and other data control functionality.

Cluster topology is determined by the fault tolerance requirements. The following operating modes are available:

| Mode | Storage<br/>volume multiplier | Minimum<br/>number<br/>of nodes | Description |
--- | --- | --- | ---
| `none` | 1 | 1 | There is no redundancy.<br/>Any hardware failure causes the storage pool to become unavailable.<br/>This mode is only recommended for functional testing. |
| `block-4-2` | 1.5 | 8 | [Erasure coding](https://en.wikipedia.org/wiki/Erasure_code) with two blocks of redundancy added to the four blocks of source data is applied. Storage nodes are placed in at least 8 failure domains (usually racks).<br/>The storage pool is available if any two domains fail, continuing to record all 6 data parts in the remaining domains.<br/>This mode is recommended for storage pools within a single availability zone (usually a data processing center). |
| `mirror-3-dc` | 3 | 9 | Data is replicated to 3 availability zones using 3 failure domains (usually racks) within each zone.<br/>The storage pool is available if one availability zone and one failure domain fail in the remaining zones.<br/>This mode is recommended for multi-data center installations. |
| `mirror-3dc-3-nodes` | 3 | 3 | It is a simplified version of `mirror-3dc`. This mode requires at least 3 servers with 3 disks each. Each server must be located in an independent data center in order to provide the best fault tolerance.<br/>Health in this mode is maintained if no more than 1 node fails.<br/>This mode is only recommended for functional testing. |

{% note info %}

Node failure means both its total and partial unavailability, for example, failure of a single disk on a node.

The storage volume multiplier specified above only applies to the fault tolerance factor. Other influencing factors (for example, slot fragmentation and granularity) must be taken into account for storage size planning.

{% endnote %}

For information about how to set the {{ ydb-short-name }} cluster topology, see [{#T}](../deploy/configuration/config.md#domains-blob).

# Bridge cluster operation mode

{% include [feature_enterprise.md](../_includes/feature_enterprise.md) %}

This article describes a special operating mode of the {{ ydb-short-name }} cluster in which data is stored with synchronous replication between multiple parts of the cluster called [pile](glossary.md#pile).

The bridge mode is an extension on top of the basic distributed storage modes: each pile is built using one of the {{ ydb-short-name }} [operating modes](topology.md#cluster-config) (for example, `mirror-3-dc` or `block-4-2`) and is fault-tolerant on its own; bridge adds synchronous replication between piles on top of that, plus managed failover/switchover of load between them. This provides an extra layer of fault tolerance on top of the chosen topologies. Bridge mode requires more hardware and is more complex to operate; it is especially useful for clusters deployed across two data centers and for systems with higher availability requirements — for example, when you need to remain available if three out of four data centers fail.

## Description of the mode {#description}

In bridge mode, cluster nodes are split into several [piles](glossary.md#pile) (typically one pile per datacenter). If one of the piles fails, the cluster becomes unavailable until the command to disable that pile is executed. After it is disabled, the cluster resumes operation.

{{ ydb-short-name }} supports an arbitrary number of piles; however, for simplicity, the following discussion considers the case of two piles.

At the [distributed storage](glossary.md#distributed-storage) level, bridge mode implements synchronous Active/Active replication between piles. At the [database node](glossary.md#database-node) level, Active/Passive redundancy is possible.

Data is stored in pairs of [storage groups](glossary.md#storage-group) from different piles. Within each pile, any {{ ydb-short-name }} [operating mode](topology.md#cluster-config) supported for data storage may be used.

Bridge mode can achieve zero-downtime [switchover](https://en.wikipedia.org/wiki/Switchover). On [failover](https://en.wikipedia.org/wiki/Failover), the cluster requires explicit disabling of replication to the failed pile before it can resume. When any pile is disabled, request processing is suspended until the command to disable replication to that pile is executed.

Piles are not standalone {{ ydb-short-name }} clusters; they are parts of a single cluster with a complex topology:

- [Tablets](glossary.md#tablet) in bridge mode run as a single instance;
- each pile runs its own [static group](glossary.md#static-group) and set of independent storage groups with regular [VDisk](glossary.md#vdisk)s. Storage groups are accessed via DS-proxy-proxy, which exposes the [DS-proxy](glossary.md#ds-proxy) interface to tablets and performs operations using two DS-proxy instances — one for the groups in each pile;
- each pile runs its own set of replicas of [StateStorage](glossary.md#state-storage), [SchemeBoard](glossary.md#scheme-board), and [Board](glossary.md#board). Access to StateStorage, SchemeBoard, and Board is through similar proxy-proxy components.

## Pile states {#pile-states}

Cluster operation is determined by the states of all its piles. For example, for a cluster of pile A and pile B, the pile states are written as (A, B), where order matters.

![Cluster state diagram](_assets/bridge_pile_states.drawio.svg)

Each pile can be in one of the following states:

- `PRIMARY`. The main pile where tablets run. Only one pile can be in the `PRIMARY` state.

- `SYNCHRONIZED`. A follower pile whose storage groups are fully synchronized with `PRIMARY`.

- `DISCONNECTED`. Disconnected from `PRIMARY`; does not participate in operations.

- `NOT_SYNCHRONIZED`. A pile whose storage groups are not yet synchronized with `PRIMARY`. When synchronization completes, {{ ydb-short-name }} automatically transitions the pile to `SYNCHRONIZED`.

- `PROMOTED`. A pile being smoothly transitioned from `SYNCHRONIZED` to `PRIMARY`. The pile remains in this state until the transition completes, after which {{ ydb-short-name }} automatically transitions it to `PRIMARY`.

- `SUSPENDED`. A pile being smoothly transitioned to `DISCONNECTED`. When the transition completes, {{ ydb-short-name }} automatically moves the pile to `DISCONNECTED`.

Normally, a pair of piles operates in `PRIMARY/SYNCHRONIZED` or `SYNCHRONIZED/PRIMARY`. That is, one pile is `PRIMARY` (tablets run there) and the other is `SYNCHRONIZED` (all its storage groups are synchronized with `PRIMARY`). In this configuration, a write is considered committed only after it has been successfully written to the storage groups of each pile with the required redundancy.


### Transitions between states {#transitions-between-states}

| From state | To state | How | Description |
| --- | --- | --- | --- |
| `PRIMARY` | `SYNCHRONIZED` | Automatically | Planned switchover complete. |
| `PRIMARY` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `PRIMARY` and selection of new `PRIMARY`. |
| `PRIMARY` | `SUSPENDED` | Manually — [takedown](../reference/ydb-cli/commands/bridge/takedown.md) | Planned disable of current `PRIMARY` and selection of new `PRIMARY`. |
| `SYNCHRONIZED` | `PRIMARY` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `PRIMARY` and selection of new `PRIMARY`. |
| `SYNCHRONIZED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `SYNCHRONIZED`. |
| `SYNCHRONIZED` | `PROMOTED` | Manually — [switchover](../reference/ydb-cli/commands/bridge/switchover.md) | Start planned change of `PRIMARY`. |
| `SYNCHRONIZED` | `SUSPENDED` | Manually — [takedown](../reference/ydb-cli/commands/bridge/takedown.md) | Start planned disable of `SYNCHRONIZED` pile. |
| `DISCONNECTED` | `NOT_SYNCHRONIZED` | Manually — [rejoin](../reference/ydb-cli/commands/bridge/rejoin.md) | Return pile to cluster after maintenance or recovery. |
| `NOT_SYNCHRONIZED` | `SYNCHRONIZED` | Automatically | Data synchronization complete. |
| `NOT_SYNCHRONIZED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `NOT_SYNCHRONIZED`. |
| `PROMOTED` | `PRIMARY` | Automatically | Planned switchover complete. |
| `PROMOTED` | `PRIMARY` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `PRIMARY` and selection of new `PRIMARY`. |
| `PROMOTED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `PROMOTED`. |
| `SUSPENDED` | `NOT_SYNCHRONIZED` | Manually — [rejoin](../reference/ydb-cli/commands/bridge/rejoin.md) | Cancel planned disable of pile. |
| `SUSPENDED` | `DISCONNECTED` | Automatically | Planned disable complete. |
| `SUSPENDED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disable of unavailable `SUSPENDED`. |


## State change scenarios {#state-change-scenarios}

### Pile failure {#failover}

When a pile fails, the cluster becomes unavailable. To resume cluster operation, the failed pile must be disabled by moving it to the `DISCONNECTED` state.

The administrator can issue a transition from `PRIMARY/SYNCHRONIZED` or `SYNCHRONIZED/PRIMARY` to either `PRIMARY/DISCONNECTED` or `DISCONNECTED/PRIMARY`. The command updates the storage group configuration and changes how data is written to those groups. [Interconnect](glossary.md#actor-system-interconnect) tears down sessions with all nodes of the pile in `DISCONNECTED`; data exchange sessions are not established (TCP connections for exchanging pile state metadata may remain active). Subsequent read and write operations run without the pile in `DISCONNECTED`. If the `PRIMARY` pile has changed, tablets are restarted on it.

The same kind of transition applies when the failed pile was in any state and the pile being designated `PRIMARY` is in one of the allowed states: `PRIMARY`, `SYNCHRONIZED`, or `PROMOTED`.

If the pile being designated `PRIMARY` was in `DISCONNECTED`, `NOT_SYNCHRONIZED`, or `SUSPENDED`, a normal transition is not possible, because that pile may not hold a complete, up-to-date replica of the data.

### Pile recovery {#rejoin}

After the nodes of the failed pile are back up, the pile must be reattached to the cluster by moving it to `NOT_SYNCHRONIZED`.

The administrator can issue a transition from `PRIMARY/DISCONNECTED` to `PRIMARY/NOT_SYNCHRONIZED` or from `DISCONNECTED/PRIMARY` to `NOT_SYNCHRONIZED/PRIMARY`. Configuration is exchanged between nodes; interconnect sessions are established only between nodes with the same configuration. The command starts storage group synchronization. When synchronization finishes, an automatic transition occurs from `PRIMARY/NOT_SYNCHRONIZED` to `PRIMARY/SYNCHRONIZED` or from `NOT_SYNCHRONIZED/PRIMARY` to `SYNCHRONIZED/PRIMARY`.

### Planned PRIMARY pile switchover {#switchover}

To change the `PRIMARY` pile in a planned way, the pile that will become the new `PRIMARY` must be moved to the `PROMOTED` state.

The administrator can issue a transition from `PRIMARY/SYNCHRONIZED` to `PRIMARY/PROMOTED` or from `SYNCHRONIZED/PRIMARY` to `PROMOTED/PRIMARY`. The command does not change how data is written to storage groups but updates their configuration and initiates the change of `PRIMARY` pile with a smooth migration of tablets to the new `PRIMARY`. When the transition completes, the former `PRIMARY` becomes `SYNCHRONIZED` and `PROMOTED` becomes `PRIMARY`.

### Planned pile disable {#takedown}

To disable a pile in a planned way, it must be moved to the `SUSPENDED` state.

The administrator can issue a transition from `PRIMARY/SYNCHRONIZED` to `PRIMARY/SUSPENDED` or from `SYNCHRONIZED/PRIMARY` to `SUSPENDED/PRIMARY`. A planned disable of the nodes of the pile moved to `SUSPENDED` is performed, and they are moved to `DISCONNECTED`. The system aims to minimize the impact of this process on cluster operation. After the pile has been moved to `DISCONNECTED`, its nodes can be shut down for maintenance.

Then perform normal pile recovery by moving it to `NOT_SYNCHRONIZED`.

### Recovery from cluster split with incompatible configuration (split brain) {#split-brain}

A situation can occur where the administrator has put the cluster in a state where pile A and pile B are isolated from each other, and then pile A was reconfigured so that it became `PRIMARY` and pile B became `SYNCHRONIZED`, while at the same time pile B was reconfigured so that it became `PRIMARY` and pile A became `SYNCHRONIZED`. This results in a cluster split with incompatible configuration ([split-brain](https://en.wikipedia.org/wiki/Split-brain_(computing))). In this state, each part of the cluster may remain operational.

To restore a single cluster, choose which pile will be wiped. Then stop all nodes of that pile, wait for them to stop completely, wipe all disks on all nodes, and bring the nodes of the pile being wiped back up.
Then perform normal pile recovery by moving it to `NOT_SYNCHRONIZED`.

### Non-standard pile recovery

In complex situations (for example, after sequential pile failures), the following scenario can occur: first pile A failed, was moved to `DISCONNECTED`, and the cluster continued operating; then pile B failed irreversibly; after that, pile A was recovered. In such cases, the cluster can be recovered based on pile A.

If the cluster must be brought back up when only a pile in `DISCONNECTED`, `NOT_SYNCHRONIZED`, or `SUSPENDED` is available, the administrator can run a transition from `PRIMARY/DISCONNECTED` to `DISCONNECTED/PRIMARY` (or similarly for `NOT_SYNCHRONIZED` or `SUSPENDED`) with the special `force` parameter. This causes a cluster split with incompatible configuration. Depending on the actual state of data in the pile being moved to `PRIMARY`, the cluster may be restored to a correct or internally inconsistent state. Correct operation of such a cluster is not guaranteed.

If the cluster turns out to be operational, you can proceed to restore it to normal state. Before recovering the disabled pile, fully wipe data and metadata on all its nodes, then move the pile to `NOT_SYNCHRONIZED`.

## Bridge mode implementation details {#bridge-implementation-details}

For more on how bridge mode works, see [{#T}](../contributor/bridge.md).

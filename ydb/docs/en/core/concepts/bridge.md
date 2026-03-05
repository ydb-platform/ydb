# Bridge Cluster Operating Mode

{% include [feature_enterprise.md](../_includes/feature_enterprise.md) %}

This article describes a special {{ ydb-short-name }} cluster operating mode in which data is stored with synchronous replication between multiple cluster parts called [pile](glossary.md#pile).

The bridge mode is an add-on over the basic distributed storage operating modes: each pile is built using one of the {{ ydb-short-name }} supported [operating modes](topology.md#cluster-config) (for example, `mirror-3-dc` or `block-4-2`) and is fault-tolerant on its own; bridge adds synchronous replication between pile and managed failover/switchover of load between them on top of that. This provides an additional layer of fault tolerance over the chosen topologies. The bridge mode requires more hardware and is more complex to operate; it is particularly useful for clusters deployed in two data centers and systems with increased availability requirements — for example, when it is necessary to maintain availability when three out of four data centers fail.

## Mode description {#description}

In bridge mode, cluster nodes are divided into several [pile](glossary.md#pile) (typically one pile per data center). If one of the pile fails, the cluster becomes unavailable until a command is executed to disconnect that pile. After disconnection, the cluster resumes operation.

{{ ydb-short-name }} supports an arbitrary number of pile; however, for simplicity, the case with two pile is considered below.

At the [distributed storage](glossary.md#distributed-storage) level, bridge mode implements synchronous Active/Active replication between pile. At the [database node](glossary.md#database-node) level, Active/Passive redundancy is possible.

Data storage uses pairs of [storage groups](glossary.md#storage-group) from different pile. Any {{ ydb-short-name }} supported [operating modes](topology.md#cluster-config) can be used for data storage in each pile.

Zero-downtime [switchover](https://en.wikipedia.org/wiki/Switchover) can be achieved in bridge mode. For [failover](https://en.wikipedia.org/wiki/Failover), the cluster requires explicit replication disconnection in the failed pile to resume operation. When any pile is disconnected, request servicing is suspended until the replication disconnection command is executed for that pile.

Pile are not standalone {{ ydb-short-name }} clusters; they are parts of a single cluster with complex topology:

- [tablets](glossary.md#tablet) in bridge mode run in a single instance;
- each pile runs a separate [static group](glossary.md#static-group) and a set of independent storage groups with regular [VDisk](glossary.md#vdisk). Access to storage groups is through DS-proxy-proxy, which provides tablets with the [DS-proxy](glossary.md#ds-proxy) interface and performs operations using two DS-proxy — one for groups in each pile;
- each pile runs its own set of [StateStorage](glossary.md#state-storage), [SchemeBoard](glossary.md#scheme-board), and [Board](glossary.md#board) replicas. Access to StateStorage, SchemeBoard, and Board is through similar proxy-proxy.

## Pile states {#pile-states}

The cluster operating mode is determined by the states of all its pile. For example, for a cluster with pile A and pile B, pile states are indicated as (A, B), where order matters.

![Cluster state diagram](_assets/bridge_pile_states.drawio.svg)

Each pile can be in one of the following states:

- `PRIMARY`. The main pile where tablets run. Only one pile can be in the `PRIMARY` state.

- `SYNCHRONIZED`. A secondary pile where all storage groups are synchronized with `PRIMARY`.

- `DISCONNECTED`. Disconnected from `PRIMARY`, does not participate in operations.

- `NOT_SYNCHRONIZED`. A pile where storage groups are not yet synchronized with `PRIMARY`. When synchronization completes, {{ ydb-short-name }} automatically switches the pile to the `SYNCHRONIZED` state.

- `PROMOTED`. A pile that needs to be smoothly switched from `SYNCHRONIZED` to `PRIMARY`. The pile remains in this state until the smooth switchover completes, after which {{ ydb-short-name }} automatically switches it to the `PRIMARY` state.

- `SUSPENDED`. A pile that needs to be smoothly switched to the `DISCONNECTED` state. When the switch completes, {{ ydb-short-name }} automatically transitions the pile to the `DISCONNECTED` state.

Normally, a pair of pile operates in `PRIMARY/SYNCHRONIZED` or `SYNCHRONIZED/PRIMARY` mode. That is, one pile is `PRIMARY` with tablets running in it, the second pile is `SYNCHRONIZED` with all its storage groups synchronized with `PRIMARY`. A write operation is considered complete in this pile state only after successful write to storage groups of each pile with the required redundancy.


### Transitions between states {#transitions-between-states}

| From state | To state | How | Description |
| --- | --- | --- | --- |
| `PRIMARY` | `SYNCHRONIZED` | Automatically | Planned switchover completion. |
| `PRIMARY` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `PRIMARY` with new `PRIMARY` selection. |
| `PRIMARY` | `SUSPENDED` | Manually — [takedown](../reference/ydb-cli/commands/bridge/takedown.md) | Planned disconnection of current `PRIMARY` with new `PRIMARY` selection. |
| `SYNCHRONIZED` | `PRIMARY` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `PRIMARY` with new `PRIMARY` selection. |
| `SYNCHRONIZED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `SYNCHRONIZED`. |
| `SYNCHRONIZED` | `PROMOTED` | Manually — [switchover](../reference/ydb-cli/commands/bridge/switchover.md) | Start of planned `PRIMARY` change. |
| `SYNCHRONIZED` | `SUSPENDED` | Manually — [takedown](../reference/ydb-cli/commands/bridge/takedown.md) | Start of planned `SYNCHRONIZED` pile disconnection. |
| `DISCONNECTED` | `NOT_SYNCHRONIZED` | Manually — [rejoin](../reference/ydb-cli/commands/bridge/rejoin.md) | Returning pile to cluster after maintenance/recovery. |
| `NOT_SYNCHRONIZED` | `SYNCHRONIZED` | Automatically | Data synchronization completion. |
| `NOT_SYNCHRONIZED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `NOT_SYNCHRONIZED`. |
| `PROMOTED` | `PRIMARY` | Automatically | Planned switchover completion. |
| `PROMOTED` | `PRIMARY` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `PRIMARY` with new `PRIMARY` selection. |
| `PROMOTED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `PROMOTED`. |
| `SUSPENDED` | `NOT_SYNCHRONIZED` | Manually — [rejoin](../reference/ydb-cli/commands/bridge/rejoin.md) | Canceling planned pile disconnection. |
| `SUSPENDED` | `DISCONNECTED` | Automatically | Planned disconnection completion. |
| `SUSPENDED` | `DISCONNECTED` | Manually — [failover](../reference/ydb-cli/commands/bridge/failover.md) | Emergency disconnection of unavailable `SUSPENDED`. |


## State change scenarios {#state-change-scenarios}

### Pile failure {#failover}

When a pile fails, the cluster becomes unavailable. To resume cluster operation, the failed pile must be disconnected by transitioning it to the `DISCONNECTED` state.

The administrator can issue a switch command from `PRIMARY/SYNCHRONIZED` or `SYNCHRONIZED/PRIMARY` to one of the states `PRIMARY/DISCONNECTED` or `DISCONNECTED/PRIMARY`. The command changes the storage group configuration and leads to a change in the way data is written to these groups. [Interconnect](glossary.md#actor-system-interconnect) terminates sessions with all nodes of the pile in the `DISCONNECTED` state; data exchange sessions are not established (TCP connections for exchanging pile state metadata may remain active). Further read and write operations are performed without the participation of the pile in the `DISCONNECTED` state. If the `PRIMARY` pile has changed, tablets are restarted in it.

The switch is performed similarly if the failed pile was in any state and the designated `PRIMARY` pile is in one of the allowed states: `PRIMARY`, `SYNCHRONIZED`, `PROMOTED`.

If the designated `PRIMARY` pile was in the `DISCONNECTED`, `NOT_SYNCHRONIZED`, or `SUSPENDED` state, a normal switch is not possible because the pile may not contain a complete and up-to-date data replica.

### Pile recovery {#rejoin}

After the failed pile nodes are restored, the pile must be reconnected to the cluster by transitioning it to the `NOT_SYNCHRONIZED` state.

The administrator can issue a switch command from `PRIMARY/DISCONNECTED` to `PRIMARY/NOT_SYNCHRONIZED` or from `DISCONNECTED/PRIMARY` to `NOT_SYNCHRONIZED/PRIMARY`. Configuration exchange between nodes will occur; interconnect sessions are established only between nodes with the same configuration. The command starts storage group synchronization. When synchronization completes, an automatic switch occurs from `PRIMARY/NOT_SYNCHRONIZED` to `PRIMARY/SYNCHRONIZED` or from `NOT_SYNCHRONIZED/PRIMARY` to `SYNCHRONIZED/PRIMARY`.

### Planned PRIMARY pile switchover {#switchover}

To perform a planned `PRIMARY` pile change, the pile designated as the new `PRIMARY` must be transitioned to the `PROMOTED` state.

The administrator can issue a switch command from `PRIMARY/SYNCHRONIZED` to `PRIMARY/PROMOTED` or from `SYNCHRONIZED/PRIMARY` to `PROMOTED/PRIMARY`. The command does not change the way data is written to storage groups but updates their configuration and initiates the `PRIMARY` pile change with a smooth transfer of tablets to the new `PRIMARY`. When the switchover completes, the `PRIMARY` state changes to `SYNCHRONIZED` and `PROMOTED` to `PRIMARY`.

### Planned pile disconnection {#takedown}

To perform a planned pile disconnection, the pile must be transitioned to the `SUSPENDED` state.

The administrator can issue a switch command from `PRIMARY/SYNCHRONIZED` to `PRIMARY/SUSPENDED` or from `SYNCHRONIZED/PRIMARY` to `SUSPENDED/PRIMARY`. Planned disconnection of the pile nodes transitioned to the `SUSPENDED` state will be performed, transitioning them to the `DISCONNECTED` mode. The system strives to minimize the impact of this process on cluster operation. After the pile is transitioned to the `DISCONNECTED` state, its nodes can be shut down for maintenance.

After that, normal pile recovery must be performed by switching it to the `NOT_SYNCHRONIZED` state.

### Recovery from cluster split with incompatible configuration (split brain) {#split-brain}

A situation may occur where the administrator brought the cluster to a state where pile A and B became isolated from each other, and then pile A was reconfigured to become `PRIMARY` and pile B `SYNCHRONIZED`, while simultaneously pile B was reconfigured to become `PRIMARY` and pile A `SYNCHRONIZED`. This leads to a cluster split with incompatible configuration ([split-brain](https://en.wikipedia.org/wiki/Split-brain_(computing))). In this state, each part of the cluster may remain operational.

To recover a single cluster, you need to choose which pile will be cleared. After that, stop all nodes of this pile, wait for their complete shutdown, clear all disks on all nodes, and then turn the cleared pile nodes back on.
After that, perform normal pile recovery by transitioning it to the `NOT_SYNCHRONIZED` state.

### Non-standard pile recovery

In complex situations, for example with sequential pile failures, the following scenario may occur: first pile A failed, it was transitioned to the `DISCONNECTED` state, and the cluster continued operation; then pile B had an irreversible failure; after that, pile A was restored. In such cases, cluster recovery based on pile A is possible.

If it is necessary to resume cluster operation when only a pile in the `DISCONNECTED`, `NOT_SYNCHRONIZED`, or `SUSPENDED` state remains operational, the administrator can execute a switch command from `PRIMARY/DISCONNECTED` to `DISCONNECTED/PRIMARY` (or similarly for `NOT_SYNCHRONIZED` or `SUSPENDED`) with the special `force` parameter. This will lead to a cluster split with incompatible configuration. Depending on the actual state of data in the pile being transitioned to the `PRIMARY` state, the cluster may be recovered to a correct or internally inconsistent state. Operational reliability of such a cluster is not guaranteed.

If the cluster turns out to be operational, you can continue recovery to normal state. Before recovering the disconnected pile, you must completely clear data and metadata on all its nodes, then transition the pile to the `NOT_SYNCHRONIZED` state.

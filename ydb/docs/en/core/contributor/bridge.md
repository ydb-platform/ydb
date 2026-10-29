# Bridge mode

{% include [feature_enterprise.md](../_includes/feature_enterprise.md) %}

For a general description of the mode and use cases, see [{#T}](../concepts/bridge.md). This article is intended for {{ ydb-short-name }} developers and contributors and describes the technical implementation details of bridge mode.

{{ ydb-short-name }} supports an arbitrary number of piles; for simplicity, the following discussion considers the case of two piles, referred to as pile A and pile B.

## Configuration storage

[Distconf](../concepts/glossary.md#distributed-configuration) stores the configuration of two [static groups](../concepts/glossary.md#static-group), two sets of [state storage](../concepts/glossary.md#state-storage), as well as [scheme board](../concepts/glossary.md#scheme-board) and [board](../concepts/glossary.md#board). It also stores the cluster operating mode.

The configuration of static groups, state storage, and boards uses a storage scheme with two quorum sets:

  - Quorum A — includes only nodes from pile A. A write is considered successful if it has been applied on a majority of nodes in pile A and on the quorum of disks of the static group of pile A;
  - Quorum B — includes only nodes from pile B. A write is considered successful if it has been applied on a majority of nodes in pile B and on the quorum of disks of the static group of pile B.

If one of the piles fails, writing to the corresponding quorum becomes impossible.

Distconf uses both quorums (A and B) to store configuration:

  - Configuration with mode `PRIMARY/DISCONNECTED` is written only to quorum A;
  - Configuration with mode `DISCONNECTED/PRIMARY` is written only to quorum B;
  - All other modes are written to both quorums — A and B.

When performing failover, the administrator puts the cluster into mode `PRIMARY/DISCONNECTED` or `DISCONNECTED/PRIMARY`. In this mode, the configuration of shared components is stored only on the quorum of the `PRIMARY` pile.

For clusters with more than two piles, emergency failover can move several unavailable piles to `DISCONNECTED` in a single `UpdateClusterState` request, together with selecting a new `PRIMARY` from an already synchronized pile. Repeated `--pile` options of the [failover command](../reference/ydb-cli/commands/bridge/failover.md) produce this single update. Applying these transitions separately can leave another unavailable pile required by the intermediate configuration, preventing a quorum. Quorum checks use the new configuration: disconnected piles are excluded, while node and storage quorum requirements within the remaining participating piles stay unchanged.

To start recovery, the administrator puts the pair of the live pile and the failed pile into mode `PRIMARY/NOT_SYNCHRONIZED`. This configuration is stored on all quorums.

Writing configuration is forbidden if the new configuration is incompatible with the last one stored on the node. For example, a transition from `PRIMARY/DISCONNECTED` to `DISCONNECTED/PRIMARY` is not allowed. This prevents a situation where pile A and pile B simultaneously switch to modes that both claim the `PRIMARY` role.

To prevent pile A and pile B from interacting when configurations conflict, interconnect session establishment is blocked when such inconsistencies are detected on nodes.

When operating in bridge mode, Distconf creates a subtree in each pile, and the roots of these subtrees are attached to the Distconf leader. This minimizes the reconfiguration of Distconf node links when a pile is lost.

## Starting static groups

The configuration of all static groups is stored in Distconf. The root of the Distconf subtree in each pile can obtain the configuration and decide which quorum is required to apply it based on that configuration. A `DISCONNECTED` pile does not participate in the quorum; therefore, applying configuration without a `DISCONNECTED` pile requires quorums A and B; applying configuration `PRIMARY/DISCONNECTED` requires only quorum A; applying configuration `DISCONNECTED/PRIMARY` requires only quorum B.

When the required quorum is formed, Distconf applies the configuration, and the [Node Warden](../concepts/glossary.md#node-warden) starts the [VDisk](../concepts/glossary.md#vdisk) and [PDisk](../concepts/glossary.md#pdisk) of the static groups.

Access to the static group is provided by the `dsproxy-proxy` service, which exposes the [dsproxy](../concepts/glossary.md#ds-proxy) interface to tablets and performs operations synchronously with both groups in the pair.

The dsproxy-proxy of a static group changes its operating mode according to the configuration received from Distconf. The dsproxy-proxy configuration is stored in the configuration of the shared components.

## dsproxy-proxy behavior {#dsproxy-proxy}

In mode `PRIMARY/SYNCHRONIZED`, writes are performed synchronously to both groups. Reads are sent to both groups too, and the reply of a synchronized group having every blob asked for is taken at once. dsproxy-proxy doesn't ping the groups to watch them: it learns of a link failure from the interconnect and of a mode change from the configuration it gets from Distconf.

In mode `PRIMARY/DISCONNECTED`, reads and writes are performed only in the `PRIMARY` group.

A node learns that a pile has been disconnected when the new configuration reaches it. Until then, its dsproxy-proxy keeps writing to the disconnected pile and reading from it, and the disconnected pile can miss writes already made by nodes that know about the disconnect. Generation numbers can't fence it, because the disconnected pile never learns the new ones. So dsproxy-proxy never takes the absence of a blob in one pile as the answer: a read that doesn't find a blob, and every range read, waits for the replies of all piles it was sent to and returns the union of what they hold. A blob found in any pile is returned; a blob is reported missing only when no pile has it, and an error from a pile makes the read fail rather than report the blob missing. Assimilation and integrity checks still take the reply of one pile holding the whole group. For a planned disconnect, when the pile stays reachable, the window is bounded by how fast the configuration reaches each node; once a node gets it, its interconnect also tears down the sessions to the disconnected pile. In an unplanned failover, the disconnected pile is unreachable, so the window doesn't arise.

In mode `PRIMARY/NOT_SYNCHRONIZED`, dsproxy-proxy supports several submodes that determine which kinds of writes are performed in the `NOT_SYNCHRONIZED` group:

  - `WRITE_KEEP` — only `KEEP` flags are written;
  - `WRITE_KEEP_BARRIER_DONOTKEEP` — barriers and `KEEP` and `DO_NOT_KEEP` flags are written;
  - `WRITE_KEEP_BARRIER_DONOTKEEP_DATA` — data, barriers, and `KEEP` and `DO_NOT_KEEP` flags are written.

The configuration of a bridged group includes, for each pile:

  - the identifier of the group the pile holds;
  - the submode of that group, which tells the writes it gets while being synchronized;
  - the generation of that group's configuration.

It also includes the generation of the cluster state it has been made for. dsproxy-proxy holding an older cluster state waits for the newer one before sending the request anywhere, so the modes of piles are always taken together with the group configuration made for them.

dsproxy-proxy sends each request to the group of a pile with the generation of that group from its configuration (`ForceGroupGeneration`). The dsproxy of the group answers `RACE` when its configuration has another generation, and VDisks execute only the requests made for their exact group generation. Every change of the configuration of a bridged group, a submode change included, makes the controller (Distconf, for a static group) increase the generations of all its groups, so the requests of dsproxy-proxy that has missed the change are not executed: they come back with `RACE` and are restarted with the newer configuration. A disconnected pile doesn't learn new generations, which is why a disconnect can't be fenced this way.

To transition from mode `PRIMARY/NOT_SYNCHRONIZED` to `PRIMARY/SYNCHRONIZED`, synchronization is performed. During synchronization, missing data is copied from the `PRIMARY` group to the `NOT_SYNCHRONIZED` group, and lock information is copied in both directions.

## Bootstrap of BSController and other system tablets {#bootstrap}

The node list for starting a tablet need only specify one set of nodes that includes a sufficient number of nodes from each pile. The Bootstrapper must analyze the state storage proxy-proxy configuration and use it to determine whether the tablet can be started on a given node. If the tablet cannot be started on the node (because the node is not in the `PRIMARY` pile), the Bootstrapper must walk the tree of Bootstrappers connected to it and assign as the launcher a Bootstrapper from the `PRIMARY` pile. If no such Bootstrapper exists, the current Bootstrapper must not start the tablet.

## Configuration and startup of other tablets {#other-tablets}

Dynamic storage groups are managed by the BS Controller. Instead of a single regular [storage pool](../concepts/glossary.md#storage-pool) each time, three storage pools are created: in pool A — groups from pile A; in pool B — groups from pile B; in pool C — proxy-proxy groups formed from pairs of groups from pools A and B. Tablets managed by [Hive](../concepts/glossary.md#hive) use only the proxy-proxy groups from pool C.

Tablets are started only in the `PRIMARY` or `PROMOTED` pile. Moving tablets from `PRIMARY` to `PROMOTED` is done as smoothly as possible; moving tablets from `DISCONNECTED` to `PRIMARY` is done as quickly as possible.

## Interconnect behavior {#interconnect}

In the [interconnect](../concepts/glossary.md#actor-system-interconnect), metadata exchange is separated from session establishment. Metadata exchange occurs before session establishment and allows a pair of nodes to determine the set of interconnect features in use and to exchange configuration. The interconnect with nodes from the disconnected half of the cluster is torn down, and no connection attempts are made. Connection establishment is only possible after the pile is moved to state `PRIMARY/NOT_SYNCHRONIZED`.

{% include [career](./_includes/career.md) %}

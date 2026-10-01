# Direct Block Groups

A direct block group (DBG) is a tablet-owned allocation of data DDisks and PersistentBuffers (PBs). The BlobStorage Controller (BSC) records the allocation and chooses eligible slots. The client uses the returned layout to implement its block replication and recovery policy.

This page describes shared placement and allocation contracts. NBS partition policies such as primary and handoff roles, write quorum, region striping, and flush scheduling belong to the [partition implementation guide](https://github.com/ydb-platform/ydb/blob/main/ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/README.md).

## Three Different Identities

| Entity | Identity and Meaning |
|---|---|
| BSC DDisk pool group | A `TGroupInfo` with `IsDDisk = true`, containing slots fitted to a DDisk storage pool. It supplies resources from which logical DBGs are allocated. |
| Logical DBG | A claim keyed by `(TabletId, DirectBlockGroupId)` in `Schema::DirectBlockGroupClaims`, containing ordered data and PB slot lists. |
| DDisk/PB service | A service at `(NodeId, PDiskId, DDiskSlotId)`. A DDisk slot runs a DDisk and its own PB child under distinct actor IDs. |

There is no one-to-one correspondence between a pool's BSC groups and a tablet's logical DBGs. The allocator collects eligible VSlots across the selected pool's groups. One slot can contribute to several logical DBGs, and its data and PB roles are accounted separately.

The protocol's `DirectBlockGroupIndex` is another distinction: it selects a client's session and PB record namespace. PB stores that index in one byte, so it must be in `0..255`. It is not a BSC group ID, and the controller's `DirectBlockGroupId` is not automatically serialized as this index.

## Pool Definition and Allocation

`DefineDDiskPool` creates or updates a storage pool with the DDisk flag, erasure type `none`, a placement geometry, PDisk filters, and `NumDDiskGroups`. The normal pool-fitting machinery creates the resource groups and slots. Defining a pool does not create a tablet's DBG claims.

A client then sends `TEvControllerAllocateDDiskBlockGroup` with its tablet ID, data pool name, PB pool name, and requested operations. The controller persists the resulting allocation and updates two counters on each affected VSlot:

- `DDiskNumVChunksClaimed`: the sum of data-side virtual chunk claims.
- `PersistentBufferRefs`: the number of PB references from logical DBGs.

Both counters can be zero on an unused pool slot, and both can be positive on the same slot. A PB reference is not a reservation for a particular amount of PB data. Actual PB capacity is governed by local allocation and admission settings.

Similarly, data chunk claims are controller bookkeeping. Current allocator candidates use an upper claim bound of `Max<ui32>()`; an accepted claim does not guarantee that the physical device already has that many free chunks. The DDisk allocates data and integrity resources lazily when writes need them.

## Placement and Pairing

Data placement enforces the pool's common realm constraints and distinct failure domains. It uses claim ordering and placement counts to distribute selected DDisks across nodes, PDisks, and slots.

PB placement follows its pool's own geometry. While defining the initial PB list, the allocator uses the data DDisk at the corresponding index as a co-location hint. It first tries that data DDisk's node, then preferred nodes if supplied, then other eligible nodes. Within a node it considers PB reference counts and can prefer a different slot from the hinted data slot.

Consequently, `data[k]` and `pb[k]` are positionally paired, but neither identical slot IDs nor identical node IDs are guaranteed. A flush must use the returned PB ID rather than constructing one from the data DDisk ID.

The two roles may select the same pool or different pools. Using one pool does not force the same PDisk for the pair. Using two pools makes the role-specific candidate sets configurable; it does not by itself guarantee different physical devices if the pool filters overlap.

The number of peers is also a client contract. The compatibility allocation request derives each role's peer count from its pool geometry. The explicit operations API carries separate `NumDDisks` and `NumPersistentBuffers` fields. NBS's fixed topology and the NBS-like load actor's supported host counts should not be treated as universal DDisk protocol constants.

## Request and Response Forms

The protocol is defined in [blobstorage.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage.proto).

| Form | Purpose |
|---|---|
| `Queries` | Compatibility allocation using `DirectBlockGroupId` and `TargetNumVChunks`; the controller translates each query into a group-definition operation. |
| `DirectBlockGroupOperations` | Explicit definition, per-DDisk claim changes, reassignment, and removal of data/PB entries. |
| `Responses` | Compatibility replies with `ActualNumVChunks` and indexed `Nodes`, each containing a data ID and a PB ID when present. |
| `DirectBlockGroups` | Explicit-operation replies with separate ordered data and PB lists. |

A request must not mix `Queries` with `DirectBlockGroupOperations`. Clients must check the outer status before accepting returned topology. Explicit operations can produce different list sizes, so do not infer a fixed paired shape without validating the client-specific requirements.

Repeating a definition with unchanged claims reuses the persisted allocation. Changing topology updates `DirectBlockGroupClaims` and the affected VSlot counters in the controller transaction. The controller also maintains a per-tablet topology revision in `DirectBlockGroupTabletState` for topology-information consumers.

The tablet should persist the successful layout before constructing its data path, then reconnect to that layout after restart. Controller operations change resource claims; they do not copy user data or prove it is safe to discard a replica. The client must coordinate migration, session retirement, PB cleanup, and `TEvDeleteTabletChunks` with the corresponding topology change.

## Bring-Up

The allocated IDs point to services managed through [NodeWarden](node-warden.md). NodeWarden starts a DDisk for each configured DDisk slot. That DDisk restores its local state and creates its PB child, regardless of which logical DBG later references either role.

Clients connect separately to each selected DDisk and PB service. [DDisk sessions](ddisk.md#sessions) return opaque tokens used on normal data requests. A service becoming reachable, a session succeeding, and a client finishing PB recovery are distinct readiness conditions.

## Source and Test Map

- [cmds_ddisk.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/cmds_ddisk.cpp): pool definition and filters.
- [config_fit_groups.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/config_fit_groups.cpp): resource groups and slots.
- [ddisk.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/mind/bscontroller/ddisk.cpp): allocation transaction, placement, claims, and response conversion.
- [blobstorage_ddisk.proto](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/blobstorage_ddisk.proto): persisted allocation and DDisk request formats.
- [ut_blobstorage/ddisk.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ut_blobstorage/ddisk.cpp): controller and storage-environment integration scenarios.
- [nbs_dbg_like_load_tablet_ut.cpp](https://github.com/ydb-platform/ydb/blob/main/ydb/core/blobstorage/ut_blobstorage/ut_ddisk/nbs_dbg_like_load_tablet_ut.cpp): client allocation, restart, multiple DBG, and multiple tablet scenarios.

## See Also

- [{#T}](../distributed-storage.md)
- [{#T}](ddisk.md)
- [{#T}](persistent-buffer.md)
- [{#T}](../load-actors-nbs-dbg-like.md)

---
name: ydb-vdisk-development
description: "Implement, debug, or review VDisk internals in ydb/core/blobstorage/vdisk: Hull, blob queries, replication, compaction, and garbage collection."
---

# VDisk Development

A VDisk stores a member's blob data and metadata within a conventional BlobStorage group. Start with the [storage overview](../../../../../../docs/en/core/contributor/distributed-storage.md). Select the subsystem involved; DDisk/PersistentBuffer protocols have separate contracts.

## Source Map

Paths below are relative to `ydb/core/blobstorage/vdisk/`.

| Task | Entry points |
|---|---|
| Actor construction and front-end admission | `vdisk_actor.cpp`, `skeleton/blobstorage_skeletonfront.cpp` |
| Request dispatch and service ownership | `skeleton/blobstorage_skeleton.cpp`, `skeleton/blobstorage_skeleton.h` |
| Blob/Block/Barrier metadata and recovery | `hullop/blobstorage_hull.h`, `hulldb/`, `localrecovery/` |
| Put/Get and result handling | `skeleton/`, `query/` |
| Synchronization and recovery replication | `syncer/`, `synclog/`, `repl/`, `ingress/` |
| Large-blob allocation, space reclamation | `huge/`, `defrag/`, `chunk_keeper/` |
| Compaction and garbage collection | `hullop/`, `hulldb/`, `anubis_osiris/` |

Event definitions and group identities also live in `ydb/core/blobstorage/base/` and `ydb/core/protos/blobstorage.proto`. Read the exact event and its callers rather than inferring a contract from a similarly named load actor.

## Implementation Checks

Trace the change through ingress/group identity checks, Hull updates, PDisk log durability, and response timing where those paths are affected. Blob, Block, and Barrier records have distinct semantics; preserve generation fencing and garbage-collection visibility. For chunk or log changes, identify when state becomes recoverable and when old storage may be reclaimed.

Read the [PDisk skill](../../../../pdisk/.agents/skills/ydb-pdisk-development/SKILL.md) when the task involves the PDisk side of the interface. Read [NodeWarden](../../../../../../docs/en/core/contributor/distributed-storage/node-warden.md) for startup/reconfiguration, and the [actor guide](../../../../../../docs/en/core/contributor/actor-system/index.md) for actor lifetime changes.

## Validation

Use the nearest existing `ut/ya.make` target (for example `skeleton/ut`, `hullop/ut`, `query/ut`, `huge/ut`, or `syncer/ut`). For changes involving a real group, replication, or restart, inspect `ydb/core/blobstorage/ut_blobstorage` and its existing scenario fixtures. Use active build instructions; do not assume a mock exercises on-disk replay or inter-node recovery.

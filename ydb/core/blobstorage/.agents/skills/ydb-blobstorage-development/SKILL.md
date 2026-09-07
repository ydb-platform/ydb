---
name: ydb-blobstorage-development
description: "Navigate YDB BlobStorage architecture and trace shared contracts for DSProxy, group layout, BSC allocation, and NodeWarden lifecycle. Use component skills for DDisk, PDisk, or VDisk internals."
---

# Shared BlobStorage Development

Start with the [distributed storage overview](../../../../../docs/en/core/contributor/distributed-storage.md) and [source layout](../../../README.md). Identify the path and contract affected before selecting deeper context.

## Route by Responsibility

| Area | Context |
|---|---|
| Blob API, DSProxy, conventional group geometry | `ydb/core/base/blobstorage.h`, `ydb/core/blobstorage/dsproxy/`, `ydb/core/blobstorage/groupinfo/`, `ydb/core/blobstorage/base/` |
| NodeWarden bring-up, service registration, restart | [NodeWarden](../../../../../docs/en/core/contributor/distributed-storage/node-warden.md), `ydb/core/blobstorage/nodewarden/` |
| BSC logical DBG allocation and placement | [DirectBlockGroups](../../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md), `ydb/core/mind/bscontroller/` |
| VDisk internals | [VDisk skill](../../../vdisk/.agents/skills/ydb-vdisk-development/SKILL.md) |
| PDisk internals | [PDisk skill](../../../pdisk/.agents/skills/ydb-pdisk-development/SKILL.md) |
| DDisk or PersistentBuffer implementation | [DDisk skill](../../../ddisk/.agents/skills/ydb-ddisk-development/SKILL.md) |
| Low-level I/O library | [PDisk I/O skill](../../../../../library/pdisk_io/.agents/skills/ydb-pdisk-io-development/SKILL.md) |

Load a component skill only when that component is involved. A DDisk/PB protocol is not a VDisk protocol, and a logical DBG is not a conventional erasure-coded BlobStorage group.

## Trace Cross-Component Changes

1. Identify the request producer, schema/event definition, consumer, response, and durable state. Check generation/owner fencing and failure responses at the boundary.
2. For layout or lifecycle changes, trace BSC configuration through NodeWarden to the actor and service mapping. Separate allocation claims from physical chunk ownership.
3. Test the changed boundary using focused component tests and, where needed, `ydb/core/blobstorage/ut_blobstorage`. Inspect that target's `ya.make` and relevant test suite before choosing filters.

For actor lifecycle/event changes, consult the [actor guide](../../../../../docs/en/core/contributor/actor-system/index.md). For tests, use active workspace build instructions. Maintain shared contracts in contributor pages and source navigation in component READMEs.

---
name: ydb-ddisk-development
description: "Implement, debug, or review DDisk and PersistentBuffer protocols in ydb/core/blobstorage/ddisk: sessions, block I/O, PB records, erase, and recovery."
---

# DDisk and PersistentBuffer Development

Start with the [DDisk source map](../../../README.md) and [DDisk contract](../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md). DDisk is separate from conventional VDisk storage. PersistentBuffer is a DDisk-owned actor with its own record and recovery semantics.

## Load Context by Task

- PB writes/reads, batching, coordinator fan-out, erase, or replay: [PersistentBuffer](../../../../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md).
- Pool allocation, identity, pairing, or claims: [DirectBlockGroups](../../../../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md).
- Actor creation, service IDs, or restart: [NodeWarden](../../../../../../docs/en/core/contributor/distributed-storage/node-warden.md).
- Router registration, I/O completion, or shutdown: [PDisk I/O source guide](../../../../../../library/pdisk_io/README.md) and, when the task involves the library's contract, its [skill](../../../../../../library/pdisk_io/.agents/skills/ydb-pdisk-io-development/SKILL.md).
- NBS producer quorum, role selection, dirty map, or flush/erase policy: [direct-partition guide](../../../../../nbs/cloud/blockstore/libs/storage/partition_direct/README.md).
- NBS-like load generator: [local design](../../../../../load_test/rfc/nbs_dbg_like/README.md) and [usage](../../../../../../docs/en/core/contributor/load-actors-nbs-dbg-like.md).

## Follow the Operation

Inspect `ydb/core/blobstorage/ddisk/ddisk.h`, the corresponding implementation handler, and `ydb/core/protos/blobstorage_ddisk.proto`. Trace credentials/session validation, selector and checksum validation, chunk ownership, durable metadata, completion, and the failure response relevant to the changed path.

For PB identity or erase changes, account for TabletId, Generation, DirectBlockGroupIndex, and Lsn. LSN production and quorum are caller policy. For sync, inspect the destination pull path and the selected DDisk/PB source segment. For allocation, distinguish a BSC claim from lazily materialized physical chunks.

For actor/callback lifetime changes, use the [actor guide](../../../../../../docs/en/core/contributor/actor-system/index.md). Keep the operation's durable boundary explicit when documenting success, cancellation, and recovery.

## Validation

Inspect `ydb/core/blobstorage/ddisk/ut` and the focused integration target `ydb/core/blobstorage/ut_blobstorage/ut_ddisk`. Longer PDisk-backed I/O/recovery scenarios are in `ydb/core/blobstorage/ddisk/ut_large`. Cover the changed format, replay, stale-session, checksum, or erase behavior using existing fixtures. Add I/O-library tests only if its interface changes. Follow active workspace build instructions.

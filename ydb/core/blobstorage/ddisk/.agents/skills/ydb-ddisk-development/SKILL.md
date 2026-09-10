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

## Shutdown and Restart Invariants

Preserve these invariants in every lifecycle change; see the [shutdown and restart contract](../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md#shutdown-and-restart).

1. DDisk and PB always drain their accepted asynchronous router I/O before acknowledging shutdown. Callback retirement and result processing must finish; actor-owned PDisk fallback requests are canceled with terminal replies before Gone. Normal shutdown has no deadline.
2. DDisk also waits for its concrete PB incarnation's `TEvGone` before publishing its own Gone. Tracked poison nondelivery accounts for an already absent child. PB releases its router reference before notifying DDisk; DDisk releases its reference before notifying NodeWarden.
3. For a NodeWarden-requested PDisk restart, NodeWarden first requests DDisk shutdown, DDisk requests PB shutdown, and only after all affected DDisks are Gone does NodeWarden permit PDisk restart. Fence replacement DDisk/PB startup during this handoff.
4. A replacement PDisk must not start device I/O until the previous PDisk's I/O has retired. `TPDisk::Stop()` synchronously stops the shared router, including publisher/callback retirement, issuer/ring retirement, and duplicated descriptor closure, then stops the source block device. Retaining an old router client must not prolong access to the device.
5. An independent PDisk stop/restart must preserve the same I/O barrier even while DDisk/PB clients remain. The router closes admission; rejected submissions make those actors enter Stopping and drain, as does PDisk session loss. This is not an unsolicited notification to idle actors. The PDisk barrier waits for their accepted router I/O and callbacks, not their actor Gone; poison remains necessary for actor death and Warden notification.

Keep the 60-second stalled-I/O diagnostic and 10-second forced-destructor fail-stop deadline separate from the indefinite normal shutdown wait. Do not replace a drain with a timeout that releases live callback or kernel-owned resources.

## Documentation Updates

Read linked documentation as context. During code changes, update documentation only when the change alters a documented contract or makes an existing statement inaccurate, or when the user explicitly requests documentation work. For contributor pages under `ydb/docs/en/` or `ydb/docs/ru/`, update the corresponding path in the other language in the same change.

Delegate needed documentation updates to a subagent with forked conversation context (`fork_turns: "all"` when supported). Give it the affected paths, the final behavior change, and relevant validation results; scope its edits to documentation. Review its changes for accuracy and English/Russian consistency before completing the task.

## Validation

Inspect `ydb/core/blobstorage/ddisk/ut` and the focused integration target `ydb/core/blobstorage/ut_blobstorage/ut_ddisk`. Longer PDisk-backed I/O/recovery scenarios are in `ydb/core/blobstorage/ddisk/ut_large`. Cover the changed format, replay, stale-session, checksum, or erase behavior using existing fixtures. Add I/O-library tests only if its interface changes. Follow active workspace build instructions.

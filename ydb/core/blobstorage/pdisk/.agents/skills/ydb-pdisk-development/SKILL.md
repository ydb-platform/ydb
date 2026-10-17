---
name: ydb-pdisk-development
description: "Implement, debug, or review PDisk log/chunk durability, owner sessions, scheduling, and recovery in ydb/core/blobstorage/pdisk. Reusable I/O backend mechanics belong to ydb/library/pdisk_io."
---

# PDisk Internals

PDisk manages physical-disk resources and the local log/chunk interface used by storage actors. Start with the PDisk section of the [storage overview](../../../../../../docs/en/core/contributor/distributed-storage.md).

## Source Map

Paths below are relative to `ydb/core/blobstorage/pdisk/`.

| Responsibility | Source |
|---|---|
| Client events and commit records | `blobstorage_pdisk.h` |
| Actor admission, bootstrap, restart | `blobstorage_pdisk_actor.cpp` |
| PDisk state and request processing | `blobstorage_pdisk_impl.h`, `blobstorage_pdisk_impl.cpp` |
| Request ownership and completions | `blobstorage_pdisk_requestimpl.h`, `blobstorage_pdisk_completion_impl.h` |
| Worker threads and block device | `blobstorage_pdisk_thread.h`, `blobstorage_pdisk_blockdevice.h` |
| Disk formats and configuration | `blobstorage_pdisk_data.h`, `blobstorage_pdisk_config.h` |
| Unit tests and mocks | `ut/`, `mock/` |

## Trace the Changed Contract

Read the producer as well as the PDisk handler. Track owner identity and OwnerRound, request ownership through worker/completion paths, and which completion is sent to which sender/cookie. Actor mailbox serialization does not serialize PDisk's worker and device threads.

For persistence changes, distinguish reservation, writing, log commit, deletion/decommit, and forgetting a chunk. Determine which record makes the transition durable and how replay reconstructs it. Respect starting points and FirstLsnToKeep when changing log retention. Check restart, stale-owner requests, read-only/error paths, and outstanding I/O during shutdown when relevant.

The [PDisk I/O skill](../../../../../../library/pdisk_io/.agents/skills/ydb-pdisk-io-development/SKILL.md) covers reusable I/O machinery in `ydb/library/pdisk_io`; load it when that library's contract is involved. A DDisk client change may also require the [DDisk contract](../../../../../../docs/en/core/contributor/distributed-storage/ddisk.md). Consult [NodeWarden](../../../../../../docs/en/core/contributor/distributed-storage/node-warden.md) for process-level bring-up and restart.

## Validation

Start with `ydb/core/blobstorage/pdisk/ut` and its existing device fixtures. Inspect whether the selected test uses a mock or real PDisk and which backend it exercises. Use `ydb/core/blobstorage/ut_blobstorage` for a changed client/recovery boundary. Follow active build instructions; choose a sanitizer or fault-injection scenario when the changed lifetime or thread interaction warrants it.

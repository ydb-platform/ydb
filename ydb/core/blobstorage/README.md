# BlobStorage Source Map

Start with the [distributed storage contributor guide](../../docs/en/core/contributor/distributed-storage.md) for architecture and shared contracts.

| Directory | Responsibility |
|---|---|
| `dsproxy/` | Client-facing blob-group requests and redundancy policy. |
| `vdisk/` | Blob parts, local indexes, synchronization, garbage collection, and recovery. |
| `pdisk/` | Device ownership, logging, chunk allocation, scheduling, and physical I/O. |
| `ddisk/` | Direct block access, integrity state, and PersistentBuffer staging. |
| `nodewarden/` | Local service lifecycle and configuration. |
| `ut_blobstorage/` | Storage-environment and cross-component integration tests. |

Related code lives outside this directory:

- `ydb/core/mind/bscontroller`: placement, pool configuration, and group claims.
- `ydb/core/base` and `ydb/core/protos`: shared events and persisted/wire formats.
- `ydb/library/pdisk_io`: low-level asynchronous I/O, including `TUringRouter`.
- `ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct`: NBS direct block replication policy.

Use the relevant component's tests first. The [NodeWarden](../../docs/en/core/contributor/distributed-storage/node-warden.md) and [direct block group](../../docs/en/core/contributor/distributed-storage/direct-block-groups.md) guides explain boundaries that commonly require integration coverage.

# DDisk and PersistentBuffer Source Map

The canonical developer contracts are in the [DDisk](../../../docs/en/core/contributor/distributed-storage/ddisk.md), [PersistentBuffer](../../../docs/en/core/contributor/distributed-storage/persistent-buffer.md), and [direct block group](../../../docs/en/core/contributor/distributed-storage/direct-block-groups.md) contributor pages.

| Task | Entry Points |
|---|---|
| Public requests and payload helpers | `ddisk.h`, `ddisk.cpp`, `ydb/core/protos/blobstorage_ddisk.proto` |
| Actor state, completions, and shutdown | `ddisk_actor.h`, `ddisk_actor.cpp` |
| Session validation | `ddisk_actor_connect.cpp` |
| PDisk owner recovery and PB child creation | `ddisk_actor_boot.cpp` |
| Chunk mappings, allocation, and deletion | `ddisk_actor_chunks.cpp` |
| Data read/write | `ddisk_actor_read_write.cpp`, `direct_io_op.{h,cpp}` |
| Integrity format and state | `ddisk_checksums.{h,cpp}`, `integrity_manager.{h,cpp}` |
| Unified sync and overlap ordering | `ddisk_actor_sync.cpp`, `segment_manager.{h,cpp}` |
| PB record layout and execution | `persistent_buffer{,_header}.h`, `ddisk_actor_persistent_buffer.cpp` |
| PB allocation and erase state | `persistent_buffer_space_allocator.*`, `persistent_buffer_barriers_manager.*` |
| PB fan-out and partial replies | `write_persistent_buffers_request_actor.*` |
| Monitoring | `ddisk_actor_mon.cpp`, `persistent_buffer_mon.*` |

`ut/` contains focused actor, integrity, sync, batching, barrier, and allocator tests. `ut_large/` contains longer PDisk-backed scenarios. Cross-component allocation and load-actor scenarios live in `../ut_blobstorage/` and `../ut_blobstorage/ut_ddisk/`.

Review session identity, delayed replies, physical ownership, and replay together when changing a persistent operation. NBS quorum, role rotation, dirty-map routing, and flush scheduling are owned by the [partition implementation](../../nbs/cloud/blockstore/libs/storage/partition_direct/README.md); the [load actor](../../load_test/rfc/nbs_dbg_like/README.md) has its own workload policy.

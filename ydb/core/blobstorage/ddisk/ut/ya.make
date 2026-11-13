UNITTEST_FOR(ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/crypto
    ydb/core/testlib/actors
    ydb/core/util/actorsys_test
)

SRCS(
    chunk_manager_ut.cpp
    ddisk_actor_ut.cpp
    metric_rates_ut.cpp
    ddisk_actor_batch_write_ut.cpp
    ddisk_actor_checksum_ut.cpp
    ddisk_actor_pdisk_ut.cpp
    ddisk_sync_ut.cpp
    integrity_manager_ut.cpp
    persistent_buffer_mon_ut.cpp
    persistent_buffer_barriers_manager_ut.cpp
    persistent_buffer_space_allocator_ut.cpp
    tablet_stats_ut.cpp
)

END()

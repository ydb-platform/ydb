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
    ddisk_actor_ut.cpp
    ddisk_actor_pdisk_ut.cpp
    ddisk_sync_ut.cpp
    segment_manager_ut.cpp
)

END()

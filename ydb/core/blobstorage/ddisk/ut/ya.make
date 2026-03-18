UNITTEST_FOR(ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/util/actorsys_test
)

SRCS(
    ddisk_actor_ut.cpp
    segment_manager_ut.cpp
)

END()

UNITTEST_FOR(ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

# Keep each long-running scenario in its own timeout budget.
SPLIT_FACTOR(23)

SIZE(LARGE)

TAG(ya:fat)

REQUIREMENTS(cpu:4 ram:8)

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/crypto
    ydb/core/testlib/actors
    ydb/core/util/actorsys_test
)

SRCS(
    persistent_buffer_benchmark_ut.cpp
    ddisk_actor_pdisk_large_ut.cpp
    ddisk_actor_pdisk_sync_ut.cpp
)

END()

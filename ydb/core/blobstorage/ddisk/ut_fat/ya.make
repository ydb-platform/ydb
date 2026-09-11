UNITTEST_FOR(ydb/core/blobstorage/ddisk)

FORK_SUBTESTS()

# Keep each restart scenario in its own timeout budget.
SPLIT_FACTOR(6)

SIZE(LARGE)

TAG(ya:fat)

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/crypto
    ydb/core/testlib/actors
    ydb/core/util/actorsys_test
)

SRCS(
    ddisk_actor_pdisk_fat_ut.cpp
)

END()

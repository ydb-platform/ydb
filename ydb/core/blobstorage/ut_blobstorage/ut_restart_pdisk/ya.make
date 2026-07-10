UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()
SPLIT_FACTOR(12)

SIZE(MEDIUM)

SRCS(
    restart_pdisk.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()

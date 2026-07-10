UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()
SPLIT_FACTOR(10)

SIZE(MEDIUM)

SRCS(
    read_only_pdisk.cpp
)

PEERDIR(
    ydb/core/blobstorage/ut_blobstorage/lib
)

END()

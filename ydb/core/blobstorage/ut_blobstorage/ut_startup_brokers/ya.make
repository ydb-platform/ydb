UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

FORK_SUBTESTS()
SPLIT_FACTOR(2)

    SIZE(MEDIUM)

    SRCS(
        startup_brokers.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

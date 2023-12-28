UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
    SKIP_TEST(VDisks balancing is not implemented yet)

    SIZE(MEDIUM)

    TIMEOUT(600)

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

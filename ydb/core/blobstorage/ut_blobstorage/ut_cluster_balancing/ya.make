UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    SRCS(
        cluster_balancing.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

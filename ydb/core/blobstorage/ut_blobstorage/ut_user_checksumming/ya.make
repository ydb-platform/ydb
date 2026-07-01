UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(SMALL)

    SRCS(
        user_checksumming.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

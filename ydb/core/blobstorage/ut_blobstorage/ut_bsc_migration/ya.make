UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(SMALL)

    SRCS(
        bsc_migration.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
REQUIREMENTS(cpu:1)

    SIZE(SMALL)

    SRCS(
        bsc_migration.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

    YQL_LAST_ABI_VERSION()

END()

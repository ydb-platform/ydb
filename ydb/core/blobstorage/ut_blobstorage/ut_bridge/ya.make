UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SRCS(
        bridge_get.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


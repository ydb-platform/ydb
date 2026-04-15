UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)

    FORK_SUBTESTS()

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

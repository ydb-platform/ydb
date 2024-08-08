UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    TIMEOUT(600)

    SRCS(
        balancing.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

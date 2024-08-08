UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

    TIMEOUT(600)

    SRCS(
        donor.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

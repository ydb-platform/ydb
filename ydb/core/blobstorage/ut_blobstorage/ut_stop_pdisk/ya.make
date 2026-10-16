UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    SRCS(
        stop_pdisk.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

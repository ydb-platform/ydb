UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SRCS(
        check_integrity.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


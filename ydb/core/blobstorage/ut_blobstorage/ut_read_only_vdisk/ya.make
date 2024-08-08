UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

    TIMEOUT(600)

    SRCS(
        read_only_vdisk.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
        ydb/core/load_test
    )

END()

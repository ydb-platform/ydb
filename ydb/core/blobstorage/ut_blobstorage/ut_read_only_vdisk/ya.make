UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    SRCS(
        read_only_vdisk.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
        ydb/core/load_test
    )

END()

UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2 ram:16)

    SRCS(
        out_of_space.cpp
        tenant_hive_oos.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

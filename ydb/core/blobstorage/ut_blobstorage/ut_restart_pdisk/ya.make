UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)
REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    SRCS(
        restart_pdisk.cpp
        pdisk_subsystem_ut.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

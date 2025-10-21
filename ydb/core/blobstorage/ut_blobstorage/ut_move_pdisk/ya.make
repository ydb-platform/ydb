UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    SRCS(
        move_pdisk.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

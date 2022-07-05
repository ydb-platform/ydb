UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    OWNER(g:kikimr)

    FORK_SUBTESTS()

    SIZE(MEDIUM)

    TIMEOUT(600)

    SRCS(
        donor.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()

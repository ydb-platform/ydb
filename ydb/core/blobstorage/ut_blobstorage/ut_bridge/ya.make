UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        bridge_data_kind.cpp
        bridge_get.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


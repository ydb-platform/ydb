UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    SRCS(
        statestorage.cpp
        statestorage_2_ring_groups.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


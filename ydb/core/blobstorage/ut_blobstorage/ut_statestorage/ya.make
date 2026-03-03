UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)

    FORK_SUBTESTS()

    SRCS(
        statestorage.cpp
        statestorage_2_ring_groups.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


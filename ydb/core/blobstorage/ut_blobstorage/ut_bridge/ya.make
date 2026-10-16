UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
<<<<<<< HEAD
=======
        bridge_assimilation.cpp
        bridge_data_kind.cpp
        bridge_failover.cpp
>>>>>>> a4a70734f34 (Bridge assimilation write fence (#55964))
        bridge_get.cpp
        bridge_rejoin.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


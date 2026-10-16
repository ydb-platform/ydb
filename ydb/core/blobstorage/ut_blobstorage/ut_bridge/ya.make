UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
<<<<<<< HEAD
=======
        bridge_cluster_state.cpp
        bridge_assimilation.cpp
        bridge_data_kind.cpp
        bridge_failover.cpp
>>>>>>> 1bb4336bc8b (Fix replacement of bridge pile synchronization state (#55927))
        bridge_get.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


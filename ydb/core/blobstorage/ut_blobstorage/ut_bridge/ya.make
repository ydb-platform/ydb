UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        bridge_assimilation.cpp
        bridge_cluster_state.cpp
        bridge_get.cpp
        bridge_restart.cpp
        bridge_syncer.cpp
        bridge_rejoin.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


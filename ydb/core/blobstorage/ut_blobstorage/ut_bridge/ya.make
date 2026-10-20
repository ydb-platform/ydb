UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        bridge_cluster_state.cpp
        bridge_add_pile.cpp
        bridge_assimilation.cpp
        bridge_conversion.cpp
        bridge_data_kind.cpp
        bridge_decommit.cpp
        bridge_failover.cpp
        bridge_get.cpp
        bridge_piles.cpp
        bridge_remove_pile.cpp
        bridge_restart.cpp
        bridge_syncer.cpp
        bridge_rejoin.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


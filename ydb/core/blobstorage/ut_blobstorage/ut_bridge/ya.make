UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        bridge_cluster_state.cpp
        bridge_add_pile.cpp
        bridge_pile_overrides_ut.cpp
        bridge_assimilation.cpp
        bridge_conversion.cpp
        bridge_data_kind.cpp
        bridge_decommit.cpp
        bridge_failover.cpp
        bridge_get.cpp
        bridge_keep_flags.cpp
        bridge_mixed_storage.cpp
        bridge_piles.cpp
        bridge_pool_mode.cpp
        bridge_remove_pile.cpp
        bridge_restart.cpp
        bridge_routing.cpp
        bridge_stale_parts.cpp
        bridge_syncer.cpp
        bridge_static_sync_lifetime.cpp
        bridge_rejoin.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


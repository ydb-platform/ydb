UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
        bridge_assimilation.cpp
        bridge_data_kind.cpp
        bridge_failover.cpp
        bridge_get.cpp
        bridge_syncer.cpp
        bridge_rejoin.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


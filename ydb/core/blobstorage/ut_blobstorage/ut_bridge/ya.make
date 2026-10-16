UNITTEST_FOR(ydb/core/blobstorage/ut_blobstorage)

    SIZE(MEDIUM)

    FORK_SUBTESTS()

    SRCS(
<<<<<<< HEAD
=======
        bridge_data_kind.cpp
        bridge_failover.cpp
>>>>>>> 3be686fc011 (Allow bridge failover after simultaneous loss of multiple piles (#55936))
        bridge_get.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/ut_blobstorage/lib
    )

END()


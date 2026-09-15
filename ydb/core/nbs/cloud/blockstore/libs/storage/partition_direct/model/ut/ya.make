UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model)

SRCS(
<<<<<<< HEAD
=======
    count_size_ut.cpp
    host_health_policy_ut.cpp
>>>>>>> de632e86bb4 (NBS-7513: Add return-to-online delay in TOracle (#52538))
    host_mask_ut.cpp
    host_roles_ut.cpp
    host_stat_ut.cpp
    oracle_ut.cpp
    region_geometry_ut.cpp
    time_predictor_ut.cpp
    vchunk_config_ut.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    library/cpp/testing/unittest
)

END()

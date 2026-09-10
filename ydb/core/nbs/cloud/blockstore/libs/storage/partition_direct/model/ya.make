LIBRARY()

GENERATE_ENUM_SERIALIZATION(host.h)
GENERATE_ENUM_SERIALIZATION(host_stat.h)
GENERATE_ENUM_SERIALIZATION(host_roles.h)
GENERATE_ENUM_SERIALIZATION(oracle.h)

SRCS(
<<<<<<< HEAD
=======
    count_size.cpp
    host_health_policy.cpp
>>>>>>> de632e86bb4 (NBS-7513: Add return-to-online delay in TOracle (#52538))
    host_mask.cpp
    host_roles.cpp
    host_stat.cpp
    host_state.cpp
    host.cpp
    oracle.cpp
<<<<<<< HEAD
=======
    oracle_config.cpp
    region_geometry.cpp
>>>>>>> de632e86bb4 (NBS-7513: Add return-to-online delay in TOracle (#52538))
    time_predictor.cpp
    vchunk_config.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/config
)

END()

RECURSE_FOR_TESTS(
    ut
)

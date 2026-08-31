LIBRARY()

GENERATE_ENUM_SERIALIZATION(host_roles.h)
GENERATE_ENUM_SERIALIZATION(host_stat.h)
GENERATE_ENUM_SERIALIZATION(host.h)
GENERATE_ENUM_SERIALIZATION(oracle.h)
GENERATE_ENUM_SERIALIZATION(vchunk_config.h)

SRCS(
    count_size.cpp
    host_mask.cpp
    host_roles.cpp
    host_stat.cpp
    host_state.cpp
    host.cpp
    mon_model.cpp
    oracle.cpp
    region_geometry.cpp
    time_predictor.cpp
    vchunk_config.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/config
)

END()

RECURSE_FOR_TESTS(
    ut
)

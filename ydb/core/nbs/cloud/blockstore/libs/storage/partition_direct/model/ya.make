LIBRARY()

GENERATE_ENUM_SERIALIZATION(host.h)
GENERATE_ENUM_SERIALIZATION(host_stat.h)
GENERATE_ENUM_SERIALIZATION(host_roles.h)

SRCS(
    host_mask.cpp
    host_roles.cpp
    host_stat.cpp
    host_state.cpp
    host.cpp
    oracle.cpp
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

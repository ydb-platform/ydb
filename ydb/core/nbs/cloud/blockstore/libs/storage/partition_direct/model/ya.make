LIBRARY()

GENERATE_ENUM_SERIALIZATION(host_state.h)

SRCS(
    host_stat.cpp
    host_state.cpp
    oracle.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/config
)

END()

RECURSE_FOR_TESTS(
    ut
)

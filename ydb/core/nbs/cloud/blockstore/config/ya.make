LIBRARY()

GENERATE_ENUM_SERIALIZATION(public.h)

SRCS(
    config.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/config/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
)

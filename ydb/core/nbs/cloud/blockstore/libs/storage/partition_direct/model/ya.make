LIBRARY()

SRCS(
    host_stat.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)

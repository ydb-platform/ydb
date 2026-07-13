LIBRARY()

SRCS(
    mon_render.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
    ut
)

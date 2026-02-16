LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/services

    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/load_actor_adapter
)

END()

RECURSE_FOR_TESTS(
    ut
)

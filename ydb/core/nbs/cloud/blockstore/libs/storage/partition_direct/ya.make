LIBRARY()

SRCS(
    direct_block_group_in_mem.cpp
    direct_block_group.cpp
    fast_path_service.cpp
    load_actor_adapter.cpp
    partition_direct_actor.cpp
    partition_direct.cpp
    request.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/bootstrap
    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport

    ydb/core/protos
    ydb/library/services

    ydb/core/mind/bscontroller
)

END()

RECURSE_FOR_TESTS(
    ut
)

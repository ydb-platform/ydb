LIBRARY()

SRCS(
    direct_block_group.cpp
    request.cpp
)

PEERDIR(
    ydb/core/mind/bscontroller

    ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/storage_transport
)

END()

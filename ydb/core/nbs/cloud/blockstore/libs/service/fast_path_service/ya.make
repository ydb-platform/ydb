LIBRARY()

SRCS(
    fast_path_service.cpp
)

PEERDIR(
    ydb/core/blobstorage/ddisk
    ydb/core/mind/bscontroller

    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group
)

END()

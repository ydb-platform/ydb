LIBRARY()

PEERDIR(
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/protos
    ydb/core/protos
)

SRCS(
    cache_block.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

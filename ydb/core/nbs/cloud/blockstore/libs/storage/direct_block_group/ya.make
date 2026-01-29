LIBRARY()

SRCS(
    direct_block_group.cpp
    request.cpp
)

PEERDIR(
    ydb/core/mind/bscontroller
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/library/actors/core
)

END()

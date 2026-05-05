LIBRARY()

SRCS(
    bootstrap.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/vhost
    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/protos
)

END()

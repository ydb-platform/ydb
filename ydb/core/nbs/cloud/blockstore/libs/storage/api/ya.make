LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/public/api/protos
    ydb/library/actors/core
)

END()

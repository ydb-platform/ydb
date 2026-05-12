LIBRARY()

SRCS(
    service.cpp
    ss_proxy.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/public/api/protos
    ydb/core/protos
    ydb/library/actors/core
)

END()

LIBRARY()

SRCS(
    endpoint_poller.cpp
    socket_poller.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/protos
    contrib/libs/grpc
)

END()

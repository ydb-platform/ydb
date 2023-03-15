LIBRARY()

SRCS(
    cancelation.cpp
)

PEERDIR(
    ydb/core/grpc_services/cancelation/protos
    ydb/core/grpc_services/base
    ydb/core/protos
)

END()

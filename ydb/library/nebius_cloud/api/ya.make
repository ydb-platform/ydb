LIBRARY()

SRCS(
    access_service.h
)

PEERDIR(
    ydb/public/api/client/nebius_private/accessservice
    ydb/library/actors/core
    ydb/library/grpc/client
    ydb/core/base
)

END()

LIBRARY()

SRCS(
    access_service.h
)

PEERDIR(
    ydb/public/api/client/nc_private/iam/v1
    ydb/library/actors/core
    ydb/library/grpc/client
    ydb/core/base
)

END()

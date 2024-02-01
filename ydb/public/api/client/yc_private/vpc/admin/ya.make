PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    admin_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
    rpc/code
    rpc/errdetails
    rpc/status
    type/timeofday
    type/dayofweek
)

PEERDIR(
    contrib/ydb/public/api/client/yc_public/api
    contrib/ydb/public/api/client/yc_private/common
    contrib/ydb/public/api/client/yc_private/operation
    contrib/ydb/public/api/client/yc_private/vpc/v1
)
END()


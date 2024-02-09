PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    abc_service.proto
    operation_service.proto
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
    ydb/public/api/client/yc_common/api
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/operation
)
END()


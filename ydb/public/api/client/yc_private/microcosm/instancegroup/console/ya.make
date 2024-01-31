PROTO_LIBRARY()

GRPC()
SRCS(
    instance_group.proto
    instance_group_service.proto
    role_service.proto
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
    ydb/public/api/client/yc_private
    ydb/public/api/client/yc_private/iam/v1
    ydb/public/api/client/yc_private/reference
)
END()


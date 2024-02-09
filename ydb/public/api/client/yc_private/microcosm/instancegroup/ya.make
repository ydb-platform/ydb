PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    admin_service.proto
    instance_group.proto
    instance_group_service.proto
    operation_service.proto
    quota_service.proto
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
    ydb/public/api/client/yc_common/api/tools
    ydb/public/api/client/yc_private/access
    ydb/public/api/client/yc_private/iam
    ydb/public/api/client/yc_private/common
    ydb/public/api/client/yc_private/operation
    ydb/public/api/client/yc_private/quota
)
END()


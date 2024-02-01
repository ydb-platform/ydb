PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    address.proto
    address_service.proto
    gateway.proto
    gateway_service.proto
    network.proto
    network_service.proto
    operation_service.proto
    quota_service.proto
    route_table.proto
    route_table_service.proto
    security_group.proto
    security_group_service.proto
    subnet.proto
    subnet_service.proto
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
    contrib/ydb/public/api/client/yc_public/api/tools
    contrib/ydb/public/api/client/yc_private/common
    contrib/ydb/public/api/client/yc_private/operation
    contrib/ydb/public/api/client/yc_private/quota
    contrib/ydb/public/api/client/yc_private/reference
)
END()


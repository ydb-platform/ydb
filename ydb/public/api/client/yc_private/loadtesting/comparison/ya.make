PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    case.proto
    chart.proto
    chart_type.proto
    chart_variant_type.proto
    comparison.proto
    monitoring.proto
    request.proto
    table.proto
    table_type.proto
    table_variant_type.proto
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
    ydb/public/api/client/yc_private/loadtesting/common
)
END()


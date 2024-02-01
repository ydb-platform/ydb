PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    case.proto
    chart.proto
    chart_type.proto
    charts.proto
    metric.proto
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
    contrib/ydb/public/api/client/yc_private/loadtesting/v1/report
)
END()


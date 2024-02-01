PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    chart_widget.proto
    dashboard.proto
    kpi.proto
    kpi_variant.proto
    sla.proto
    text_widget.proto
    title_widget.proto
    widget.proto
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
    contrib/ydb/public/api/client/yc_private/loadtesting/common
)
END()


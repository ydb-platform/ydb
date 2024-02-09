PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    codes.proto
    quantiles.proto
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


PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    create_metadata.proto
    interface.proto
    migration.proto
    status.proto
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
    ydb/public/api/client/yc_private/common
)
END()


PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    agent_selector.proto
    details.proto
    file_pointer.proto
    imbalance_point.proto
    object_storage.proto
    single_agent_configuration.proto
    status.proto
    summary.proto
    test.proto
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
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/common
)
END()


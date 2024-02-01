PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    agent_service.proto
    config_service.proto
    operation_service.proto
    report_service.proto
    test_service.proto
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
    contrib/ydb/public/api/client/yc_private/common
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/agent
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/config
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/report
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/report/table
    contrib/ydb/public/api/client/yc_private/loadtesting/api/v1/test
    contrib/ydb/public/api/client/yc_private/operation
)
END()


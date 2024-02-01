PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    agent.proto
    agent_registration_service.proto
    agent_service.proto
    greeter_service.proto
    job_service.proto
    monitoring_service.proto
    operation_service.proto
    tank_service.proto
    test.proto
    test_service.proto
    trail_service.proto
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
    contrib/ydb/public/api/client/yc_private/operation
)
END()


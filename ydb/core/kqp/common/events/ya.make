LIBRARY()

SRCS(
    events.cpp
    query.cpp
    script_executions.cpp
    workload_service.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/base
    ydb/core/grpc_services/base
    ydb/core/grpc_services/cancelation
    ydb/core/kqp/common/shutdown
    ydb/core/kqp/common/compilation
    ydb/core/resource_pools

    ydb/library/yql/dq/actors
    ydb/public/api/protos
    ydb/public/lib/operation_id

    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()

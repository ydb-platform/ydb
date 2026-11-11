YQL_LIBRARY()

SRCS(
    events.cpp
    query.cpp
    scan_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/formats/arrow
    ydb/core/grpc_services/base
    ydb/core/grpc_services/cancelation
    ydb/core/kqp/common/compilation
    ydb/core/kqp/common/shutdown
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/core/resource_pools
    ydb/core/scheme
    ydb/library/actors/core
    ydb/library/formats/arrow/validation
    ydb/library/yql/dq/actors
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
)

GENERATE_ENUM_SERIALIZATION(script_executions.h)

END()

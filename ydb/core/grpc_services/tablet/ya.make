LIBRARY()

SRCS(
    rpc_change_schema.cpp
    rpc_execute_mkql.cpp
    rpc_restart_tablet.cpp
    service_tablet.h
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/protos
    ydb/library/mkql_proto
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/public/api/protos
    library/cpp/protobuf/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

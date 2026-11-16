LIBRARY()

SRCS(
    rpc_change_schema.cpp
    rpc_execute_mkql.cpp
    rpc_restart_tablet.cpp
    rpc_reassign_tablets.cpp
    service_tablet.h
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/library/mkql_proto
    yql/essentials/minikql
    yql/essentials/minikql/computation
    ydb/public/api/protos
    library/cpp/protobuf/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

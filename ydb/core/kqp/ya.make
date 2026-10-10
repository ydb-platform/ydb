LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/engine
    ydb/core/formats
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/common
    ydb/core/kqp/compile_service
    ydb/core/kqp/compute_actor
    ydb/core/kqp/counters
    ydb/core/kqp/executer_actor
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/gateway
    ydb/core/kqp/host
    ydb/core/kqp/node_service
    ydb/core/kqp/opt
    ydb/core/kqp/provider
    ydb/core/kqp/proxy_service
    ydb/core/kqp/query_compiler
    ydb/core/kqp/rm_service
    ydb/core/kqp/runtime
    ydb/core/kqp/session_actor
    ydb/core/protos
    ydb/core/sys_view/service
    ydb/core/util
    ydb/core/ydb_convert
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/helpers
    ydb/library/actors/wilson
    ydb/library/yql/utils/actor_log
    ydb/public/api/protos
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/library/operation_id
    yql/essentials/core/services/mounts
    yql/essentials/public/issue
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt
)

END()

RECURSE(
    common
    compile_service
    compute_actor
    counters
    executer_actor
    expr_nodes
    federated_query
    gateway
    host
    node_service
    opt
    provider
    proxy_service
    rm_service
    runtime
    script_executions
    session_actor
    tests
)

RECURSE_FOR_TESTS(
    tools/cbo_latency_dataset
    tools/hash_test
    ut
)

IF (NOT OS_WINDOWS)
    RECURSE_FOR_TESTS(
        tools/combiner_perf/bin
    )
ENDIF()

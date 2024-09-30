LIBRARY()

SRCS(
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/library/actors/helpers
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/library/actors/wilson
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
    ydb/library/yql/core/services/mounts
    ydb/library/yql/public/issue
    ydb/library/yql/utils/actor_log
    ydb/library/yql/utils/log
    ydb/public/api/protos
    ydb/public/lib/base
    ydb/public/lib/operation_id
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
    finalize_script_service
    gateway
    host
    node_service
    opt
    provider
    proxy_service
    rm_service
    run_script_actor
    runtime
    session_actor
    tests
    workload_service
)

RECURSE_FOR_TESTS(
    ut
)

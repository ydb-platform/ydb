LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_request.cpp
    kqp_compile_service.cpp
    kqp_shutdown_controller.cpp 
    kqp_query_replay.h 
    kqp_ic_gateway.cpp
    kqp_ic_gateway_actors.h 
    kqp_metadata_loader.cpp 
    kqp_metadata_loader.h 
    kqp_impl.h
    kqp_response.cpp
    kqp_worker_actor.cpp
    kqp_session_actor.cpp
    kqp.h
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/client/minikql_compile
    ydb/core/engine
    ydb/core/formats
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/common
    ydb/core/kqp/compute_actor
    ydb/core/kqp/counters
    ydb/core/kqp/executer
    ydb/core/kqp/host
    ydb/core/kqp/node
    ydb/core/kqp/proxy
    ydb/core/kqp/rm
    ydb/core/kqp/runtime
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
    compile
    compute_actor
    counters
    executer
    expr_nodes
    host
    node
    opt
    prepare
    provider
    proxy
    rm
    runtime
)

RECURSE_FOR_TESTS(
    ut
)

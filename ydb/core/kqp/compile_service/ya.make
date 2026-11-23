LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
    kqp_warmup_compile_actor.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console/events
    ydb/core/kqp/common
    ydb/core/kqp/common/simple
    ydb/core/kqp/common/warmup
    ydb/core/kqp/compile_service/helpers
    ydb/core/kqp/counters
    ydb/core/kqp/federated_query
    ydb/core/kqp/host
    ydb/core/kqp/tracing
    ydb/core/path_aliasing
    ydb/core/ydb_convert
    ydb/library/actors/interconnect
    ydb/library/query_actor
    ydb/library/security
    ydb/library/ydb_issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    helpers
    obfuscate
)

RECURSE_FOR_TESTS(
    obfuscate/ut
    ut
)

LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
    kqp_warmup_compile_actor.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/common/simple
    ydb/core/kqp/counters
    ydb/core/kqp/federated_query
    ydb/core/kqp/host
    ydb/core/ydb_convert
    ydb/core/kqp/compile_service/helpers
    ydb/library/actors/interconnect
    ydb/library/query_actor
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    helpers
)

RECURSE_FOR_TESTS(
    ut
)

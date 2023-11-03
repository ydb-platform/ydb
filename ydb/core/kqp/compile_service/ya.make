LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common/simple
    ydb/core/kqp/federated_query
    ydb/core/kqp/host
    ydb/core/ydb_convert
)

YQL_LAST_ABI_VERSION()

END()

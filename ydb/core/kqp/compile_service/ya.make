LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
    kqp_compile_computation_pattern_service.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/host
    ydb/core/kqp/common/simple
    ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()

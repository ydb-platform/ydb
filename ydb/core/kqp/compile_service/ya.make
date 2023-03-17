LIBRARY()

SRCS(
    kqp_compile_actor.cpp
    kqp_compile_service.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/host
    ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()

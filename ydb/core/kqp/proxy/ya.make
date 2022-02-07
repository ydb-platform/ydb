LIBRARY()

OWNER(g:kikimr)

SRCS(
    kqp_proxy_service.cpp
    kqp_proxy_peer_stats_calculator.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/counters
    ydb/core/mind
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

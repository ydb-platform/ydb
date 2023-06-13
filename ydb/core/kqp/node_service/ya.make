LIBRARY()

SRCS(
    kqp_node_service.cpp
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
    ydb/core/tablet
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

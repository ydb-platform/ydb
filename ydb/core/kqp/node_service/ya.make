LIBRARY()

SRCS(
    kqp_node_service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/compute_actor
    ydb/core/kqp/counters
    ydb/core/mind
    ydb/core/protos
    ydb/core/tablet
    ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()

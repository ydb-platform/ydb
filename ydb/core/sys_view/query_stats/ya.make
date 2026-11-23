YQL_LIBRARY()

SRCS(
    query_metrics.h
    query_metrics.cpp
    query_stats.h
    query_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/core/sys_view/service
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)

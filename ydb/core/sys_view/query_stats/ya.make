LIBRARY()

SRCS(
    query_metrics.h
    query_metrics.cpp
    query_stats.h
    query_stats.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/core/sys_view/service
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

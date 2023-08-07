LIBRARY()

SRCS(
    yql_ut_common.cpp
    yql_ut_common.h
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/expr_nodes
)


YQL_LAST_ABI_VERSION()


END()

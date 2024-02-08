LIBRARY()

SRCS(
    yql_expr_traits.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/minikql/computation
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

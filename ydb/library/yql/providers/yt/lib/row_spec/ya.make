LIBRARY()

SRCS(
    yql_row_spec.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/ast
    ydb/library/yql/core/expr_nodes_gen
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/core/issue
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

LIBRARY()

SRCS(
    yql_result_provider.cpp
    yql_result_provider.h
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/ast
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

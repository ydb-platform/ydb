LIBRARY()

SRCS(
    yql_result_provider.cpp
    yql_result_provider.h
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/ast
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/provider
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

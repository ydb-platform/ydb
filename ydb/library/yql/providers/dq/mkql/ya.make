LIBRARY()

PEERDIR(
    yql/essentials/core
    ydb/library/yql/dq/integration
    yql/essentials/providers/common/mkql
    ydb/library/yql/providers/dq/expr_nodes
)

SRCS(
    dqs_mkql_compiler.cpp
    parser.cpp
)

YQL_LAST_ABI_VERSION()

END()

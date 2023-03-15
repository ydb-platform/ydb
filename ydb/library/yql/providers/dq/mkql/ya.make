LIBRARY()

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/dq/integration
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/dq/expr_nodes
)

SRCS(
    dqs_mkql_compiler.cpp
)

YQL_LAST_ABI_VERSION()

END()

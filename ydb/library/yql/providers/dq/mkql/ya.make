LIBRARY()

OWNER(
    g:yql
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/interface
)

SRCS(
    dqs_mkql_compiler.cpp
)

YQL_LAST_ABI_VERSION() 

END()

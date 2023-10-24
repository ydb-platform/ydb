LIBRARY()

SRCS(
    collection.cpp
    predicate_node.cpp
    settings.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/core/expr_nodes_gen
    ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

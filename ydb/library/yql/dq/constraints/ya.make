LIBRARY()

PEERDIR(
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/type_ann
    yql/essentials/core
    yql/essentials/ast
    yql/essentials/providers/common/transform
)

SRCS(
    dq_constraints.cpp
)

YQL_LAST_ABI_VERSION()

END()

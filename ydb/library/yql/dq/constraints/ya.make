LIBRARY()

PEERDIR(
    ydb/library/yql/dq/expr_nodes
    yql/essentials/core
    yql/essentials/providers/common/transform
)

SRCS(
    dq_constraints.cpp
)

YQL_LAST_ABI_VERSION()

END()

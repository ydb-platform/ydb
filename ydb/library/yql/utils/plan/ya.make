LIBRARY()

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core/expr_nodes
)

SRCS(
    plan_utils.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/expr_nodes
)

SRCS(
    plan_utils.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

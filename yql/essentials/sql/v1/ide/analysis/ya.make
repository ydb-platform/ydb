LIBRARY()

PEERDIR(
    yql/essentials/sql/v1/ide/core
    yql/essentials/sql/v1/ide/pure_ast
    contrib/libs/re2
)

SRCS(
    evaluate.cpp
    named_node_resolution.cpp
    parse_tree.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

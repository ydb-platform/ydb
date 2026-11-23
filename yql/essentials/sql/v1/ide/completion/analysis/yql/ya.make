YQL_LIBRARY()

SRCS(
    cluster.cpp
    table.cpp
    yql.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
)

END()

RECURSE_FOR_TESTS(
    ut
)

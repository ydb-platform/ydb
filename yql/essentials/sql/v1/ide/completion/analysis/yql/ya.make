LIBRARY()

SRCS(
    cluster.cpp
    table.cpp
    yql.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

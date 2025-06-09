LIBRARY()

SRCS(
    cluster.cpp
    table.cpp
    yql.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins
)

END()

RECURSE_FOR_TESTS(
    ut
)

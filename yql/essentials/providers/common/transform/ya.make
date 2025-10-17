LIBRARY()

SRCS(
    yql_exec.cpp
    yql_lazy_init.cpp
    yql_optimize.cpp
    yql_visit.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
)

END()

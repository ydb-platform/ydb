LIBRARY()

SRCS(
    yql_exec.cpp
    yql_lazy_init.cpp
    yql_optimize.cpp
    yql_visit.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
)

END()

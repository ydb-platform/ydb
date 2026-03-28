LIBRARY()

SRCS(
    cbo_simple.cpp
)

PEERDIR(
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/utils
)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

END()

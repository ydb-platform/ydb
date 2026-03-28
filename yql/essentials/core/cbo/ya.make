LIBRARY()

SRCS(
    cbo_optimizer_new.cpp
    cbo_interesting_orderings.cpp
    cbo_hints.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

END()

RECURSE(
    simple
)

RECURSE_FOR_TESTS(
    ut
)

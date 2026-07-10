UNITTEST_FOR(ydb/core/kqp/executer_actor)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_executer_ut.cpp
    kqp_tasks_graph_ut.cpp
    max_tasks_graph_ut.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

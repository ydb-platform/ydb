UNITTEST_FOR(ydb/core/control)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    library/cpp/testing/unittest
    util
    ydb/core/base
    ydb/core/mind
    ydb/core/mon
    ydb/library/yql/sql/pg_dummy
    ydb/services/ydb
    ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    immediate_control_board_ut.cpp
    immediate_control_board_actor_ut.cpp
)

END()

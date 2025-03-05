UNITTEST_FOR(ydb/core/control/lib)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    util
    ydb/core/base
)

SRCS(
    immediate_control_board_ut.cpp
)

END()

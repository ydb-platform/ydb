UNITTEST_FOR(ydb/library/actors/testlib)

FORK_SUBTESTS()
SIZE(SMALL)


PEERDIR(
    ydb/library/actors/core
)

SRCS(
    decorator_ut.cpp
    fast_time_ut.cpp
    subsystem_ut.cpp
)

END()

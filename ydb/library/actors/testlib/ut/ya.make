UNITTEST_FOR(ydb/library/actors/testlib)

FORK_SUBTESTS()
SIZE(SMALL)
REQUIREMENTS(cpu:1)


PEERDIR(
    ydb/library/actors/core
)

SRCS(
    decorator_ut.cpp
    subsystem_ut.cpp
)

END()

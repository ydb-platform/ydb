UNITTEST_FOR(ydb/library/actors/helpers)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TIMEOUT(1200)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TIMEOUT(600)
    REQUIREMENTS(
        ram:16
    )
ENDIF()


PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
    ydb/library/actors/core
)

SRCS(
    selfping_actor_ut.cpp
)

END()

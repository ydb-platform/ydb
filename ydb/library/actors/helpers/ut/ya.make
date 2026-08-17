UNITTEST_FOR(ydb/library/actors/helpers)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()


PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
    ydb/library/actors/core
)

SRCS(
    actor_liveness_checker_ut.cpp
    selfping_actor_ut.cpp
)

END()

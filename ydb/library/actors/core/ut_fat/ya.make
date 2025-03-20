UNITTEST_FOR(ydb/library/actors/core)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    SPLIT_FACTOR(20)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(LARGE)
    TAG(ya:fat)
    SPLIT_FACTOR(20)
ENDIF()


PEERDIR(
    ydb/library/actors/core
)

SRCS(
    actor_benchmark.cpp
    waiting_benchs.cpp
)

END()

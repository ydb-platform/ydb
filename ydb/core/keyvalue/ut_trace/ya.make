UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    keyvalue_ut_trace.cpp
)

REQUIREMENTS(ram:16)

END()

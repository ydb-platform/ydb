UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

SPLIT_FACTOR(5)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    keyvalue_ut_trace.cpp
)

END()

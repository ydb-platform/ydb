UNITTEST_FOR(ydb/library/arrow_kernels)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow
)

ADDINCL(
    ydb/library/arrow_clickhouse
)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_common.cpp
    ut_arithmetic.cpp
    ut_math.cpp
    ut_round.cpp
)

END()

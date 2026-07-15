UNITTEST_FOR(ydb/library/arrow_kernels)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
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

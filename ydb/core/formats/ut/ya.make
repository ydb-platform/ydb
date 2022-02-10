UNITTEST_FOR(ydb/core/formats)

OWNER(
    chertus
    g:kikimr
)

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
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow 
    ydb/core/base
)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_arrow.cpp
    ut_arithmetic.cpp
    ut_math.cpp
    ut_round.cpp
    ut_program_step.cpp
    custom_registry.cpp
)

END()

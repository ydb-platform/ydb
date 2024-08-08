UNITTEST_FOR(ydb/core/tx/balance_coverage)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ELSE()
    TIMEOUT(60)
    SIZE(SMALL)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

SRCS(
    balance_coverage_builder_ut.cpp
)

END()

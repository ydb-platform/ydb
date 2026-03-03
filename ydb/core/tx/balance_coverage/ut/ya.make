UNITTEST_FOR(ydb/core/tx/balance_coverage)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
    REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

SRCS(
    balance_coverage_builder_ut.cpp
)

END()

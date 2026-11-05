UNITTEST_FOR(ydb/core/tx/balance_coverage)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
)

SRCS(
    balance_coverage_builder_ut.cpp
)

END()

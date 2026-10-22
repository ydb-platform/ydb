UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    REQUIREMENTS(ram:32 cpu:2)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_incr_restore_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()

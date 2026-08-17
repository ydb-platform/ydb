UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()
IF (BUILD_TYPE == "DEBUG" OR SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/protos
)

SRCS(
    ut_set_column_constraint_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()

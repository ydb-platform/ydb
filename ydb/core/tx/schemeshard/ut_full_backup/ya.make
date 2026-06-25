UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
)

SRCS(
    ut_full_backup.cpp
    ut_full_backup_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()

UNITTEST_FOR(ydb/core/tx/columnshard)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/json
    library/cpp/testing/hook
    library/cpp/testing/unittest
    ydb/core/tablet_flat
    ydb/core/testlib/default
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/wrappers/ut_helpers
    ydb/library/aclib
    ydb/library/aws_init
)

YQL_LAST_ABI_VERSION()

SRCS(ut_local_db_backup.cpp)

END()

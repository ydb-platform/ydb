UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_backup_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/wrappers/ut_helpers
    ydb/public/lib/ydb_cli/dump
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_import
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_value
    ydb/library/backup
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
)

YQL_LAST_ABI_VERSION()

END()

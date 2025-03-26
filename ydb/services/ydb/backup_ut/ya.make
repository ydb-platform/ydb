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
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/public/lib/ydb_cli/dump
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/value
    ydb/library/backup
)

YQL_LAST_ABI_VERSION()

END()

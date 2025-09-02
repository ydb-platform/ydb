UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    backup_path_ut.cpp
    encrypted_backup_ut.cpp
    list_objects_in_s3_export_ut.cpp
    ydb_backup_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/streams/zstd
    ydb/core/testlib/default
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/public/lib/ydb_cli/dump
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/value
    ydb/library/backup
)

YQL_LAST_ABI_VERSION()

END()

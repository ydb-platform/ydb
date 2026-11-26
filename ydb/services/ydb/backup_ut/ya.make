UNITTEST_FOR(ydb/services/ydb)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    backup_path_ut.cpp
    encrypted_backup_ut.cpp
    fs_backup_validation_ut.cpp
    list_objects_in_s3_export_ut.cpp
    ydb_backup_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    library/cpp/regex/pcre
    library/cpp/streams/zstd
    library/cpp/testing/hook
    ydb/core/backup/common
    ydb/core/protos
    ydb/core/testlib/pg
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/library/backup
    ydb/library/backup/proto
    ydb/library/testlib/parquet_helpers
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump
    ydb/public/lib/ydb_cli/dump/files
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/value
)

YQL_LAST_ABI_VERSION()

IF (OS_LINUX)
    LDFLAGS(-Wl,--wrap=statfs)
ENDIF()

END()

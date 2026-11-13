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
    ../backup_ut/table_checksum_ut.cpp
    init.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/fmt
    library/cpp/streams/zstd
    library/cpp/testing/hook
    ydb/core/testlib/default
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/value
    ydb/library/backup
    yql/essentials/udfs/common/digest
)

YQL_LAST_ABI_VERSION()

END()

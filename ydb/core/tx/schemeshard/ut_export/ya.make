UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(11)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

IF (NOT OS_WINDOWS)
    PEERDIR(
        contrib/libs/apache/arrow
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        ydb/core/testlib/default
        ydb/library/testlib/parquet_helpers
        ydb/core/tx
        ydb/core/tx/columnshard
        ydb/core/tx/columnshard/hooks/testing
        ydb/core/tx/columnshard/private_events
        ydb/core/tx/columnshard/test_helper
        ydb/core/tx/schemeshard/ut_helpers
        ydb/core/util
        ydb/core/wrappers/ut_helpers
        ydb/library/aws_init
        yql/essentials/public/udf/service/exception_policy
        ydb/core/testlib/audit_helpers
    )
    SRCS(
        ut_export.cpp
        ut_export_fs.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

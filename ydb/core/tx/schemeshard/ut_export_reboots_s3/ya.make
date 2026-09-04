UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(18)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/public/lib/ydb_cli/dump/util
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
)

SRCS(
    ut_export_reboots_s3.cpp
)

YQL_LAST_ABI_VERSION()

END()

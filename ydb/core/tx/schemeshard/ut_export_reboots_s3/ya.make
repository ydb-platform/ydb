UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(12)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/wrappers/ut_helpers
    ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    ut_export_reboots_s3.cpp
)

YQL_LAST_ABI_VERSION()

END()

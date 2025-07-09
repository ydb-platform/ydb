UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(11)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

IF (NOT OS_WINDOWS)
    PEERDIR(
        library/cpp/getopt
        library/cpp/regex/pcre
        library/cpp/svnversion
        ydb/core/testlib/default
        ydb/core/tx
        ydb/core/tx/schemeshard/ut_helpers
        ydb/core/util
        ydb/core/wrappers/ut_helpers
        yql/essentials/public/udf/service/exception_policy
        ydb/core/testlib/audit_helpers
    )
    SRCS(
        ut_export.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    ut_index_build_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()

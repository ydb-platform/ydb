UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(50)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
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
    yql/essentials/public/udf/service/exception_policy
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    ut_fulltext_index_build_reboots.cpp
    ut_index_build_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()

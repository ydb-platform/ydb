UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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

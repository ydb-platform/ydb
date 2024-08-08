UNITTEST_FOR(ydb/core/load_test)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/load_test
    ydb/core/testlib/default
    ydb/core/tx
    ydb/library/yql/public/udf/service/exception_policy
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_result
)

YQL_LAST_ABI_VERSION()

SRCS(
    ../ut_ycsb.cpp
)

REQUIREMENTS(ram:32)

END()

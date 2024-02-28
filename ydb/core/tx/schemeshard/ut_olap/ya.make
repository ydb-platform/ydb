UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
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
    ydb/core/testlib/default
    ydb/core/formats
    ydb/core/tx
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_olap.cpp
    ydb/core/tx/columnshard/columnshard_ut_common.cpp
)

END()

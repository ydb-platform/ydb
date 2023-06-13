UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(6)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
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
    ydb/library/yql/public/udf/service/exception_policy
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_result
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_common.cpp
    datashard_ut_common.h
    datashard_ut_snapshot.cpp
)

REQUIREMENTS(ram:32)

END()

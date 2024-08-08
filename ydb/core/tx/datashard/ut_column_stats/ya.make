UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

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
    ydb/core/tx/datashard/ut_common
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_column_stats.cpp
)

REQUIREMENTS(ram:32)

END()

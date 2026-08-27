UNITTEST_FOR(ydb/core/statistics/aggregator)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/udfs/statistics_internal
    ydb/core/kqp/node_service
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/statistics/ut_common
    ydb/core/tx/conveyor_composite/usage
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
)

SRCS(
    ut_analyze.cpp
    ut_traverse.cpp
    ut_analyze_op.cpp
)

END()

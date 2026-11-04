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
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/statistics/ut_common
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
)

SRCS(
<<<<<<< HEAD
    ut_analyze_datashard.cpp
    ut_analyze_columnshard.cpp
=======
    ut_base_statistics_counters.cpp
    ut_analyze.cpp
    ut_traverse.cpp
>>>>>>> 03ddd4b6aa4 (Avoid full statistics rescans after each SchemeShard report (#58061))
    ut_analyze_op.cpp
    ut_traverse_datashard.cpp
    ut_traverse_columnshard.cpp
)

END()

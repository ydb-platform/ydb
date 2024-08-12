UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_analyze_ut.cpp
    kqp_explain_ut.cpp
    kqp_limits_ut.cpp
    kqp_params_ut.cpp
    kqp_query_ut.cpp
    kqp_stats_ut.cpp
    kqp_types_ut.cpp
)

PEERDIR(
    ydb/core/statistics/ut_common
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

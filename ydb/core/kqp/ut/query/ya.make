UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
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
    ydb/public/sdk/cpp/src/client/proto
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

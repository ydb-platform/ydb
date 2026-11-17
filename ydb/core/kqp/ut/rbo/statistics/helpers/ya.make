LIBRARY()

SRCS(
    kqp_analyzed_benchmarks.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/json
    ydb/core/statistics/ut_common
    ydb/library/yql/udfs/statistics_internal
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    yql/essentials/public/langver
    yql/essentials/udfs/common/hyperloglog
)

YQL_LAST_ABI_VERSION()

END()

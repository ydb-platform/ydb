G_BENCHMARK(ut_describe_table_bench)

TAG(ya:manual)

SIZE(MEDIUM)

# Run with a larger --benchmark_min_time (or run the binary directly) when comparing
# numbers before and after a change; a too-small min_time measures only the cold build
# at the 100K-partition args.
BENCHMARK_OPTS(--benchmark_min_time=1s)

SRCS(
    b_describe_table.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/pg
    ydb/core/tx
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/protos
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()

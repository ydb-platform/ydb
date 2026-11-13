G_BENCHMARK(ut_pq_configure_bench)

TAG(ya:manual)

SIZE(MEDIUM)

# Run the binary directly (or with a larger --benchmark_min_time) when comparing
# numbers before and after a change. A memory table (allocator-stat deltas per
# phase) is printed after the timed loop of each configuration.
BENCHMARK_OPTS(--benchmark_min_time=1s)

SRCS(
    b_pq_configure.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/mon_alloc
    ydb/core/persqueue
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/protos
    yql/essentials/public/udf/service/exception_policy
)

END()

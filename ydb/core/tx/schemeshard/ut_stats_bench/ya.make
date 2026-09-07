G_BENCHMARK(ut_stats_bench)

TAG(ya:manual)

SIZE(MEDIUM)

# Run the binary directly (or with a larger --benchmark_min_time) when comparing numbers
# before and after a change.
BENCHMARK_OPTS(--benchmark_min_time=0.05s)

SRCS(
    b_table_stats_arena.cpp
)

PEERDIR(
    ydb/core/tx/datashard
    ydb/core/protos
    ydb/library/actors/core
)

END()

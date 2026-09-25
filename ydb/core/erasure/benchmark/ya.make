G_BENCHMARK(erasure-bench)

SIZE(SMALL)

BENCHMARK_OPTS(--benchmark_min_time=0.5s)

SRCS(
    erasure_bench.cpp
)

PEERDIR(
    ydb/core/erasure
)

END()

G_BENCHMARK(path_aliasing_benchmark)

SIZE(MEDIUM)

BENCHMARK_OPTS(--benchmark_min_time=0.02s)

SRCS(
    path_normalizer_benchmark.cpp
)

PEERDIR(
    ydb/core/path_aliasing
    ydb/core/protos
)

END()

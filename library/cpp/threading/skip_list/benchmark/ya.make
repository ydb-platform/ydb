G_BENCHMARK()

PEERDIR(
    library/cpp/threading/skip_list
)

SRCS(
    skiplist_benchmark.cpp
)

BENCHMARK_OPTS(
    --benchmark_min_time=0.05s
)

SIZE(MEDIUM)

END()

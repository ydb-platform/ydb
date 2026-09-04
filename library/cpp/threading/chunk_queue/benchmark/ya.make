G_BENCHMARK()

SRCS(
    queue_benchmark.cpp
)

PEERDIR(
    library/cpp/threading/chunk_queue
)

BENCHMARK_OPTS(
    --benchmark_min_time=0.05s
)

SIZE(MEDIUM)

END()

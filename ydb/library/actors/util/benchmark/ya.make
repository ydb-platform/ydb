G_BENCHMARK()

SRCS(
    mpsc_queue_benchmark.cpp
)

PEERDIR(
    library/cpp/threading/queue
    ydb/library/actors/util
)

BENCHMARK_OPTS(
    --benchmark_min_time=0.05s
)

SIZE(SMALL)

END()

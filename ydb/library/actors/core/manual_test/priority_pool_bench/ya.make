G_BENCHMARK(executor_pool_priority_bench)

SRCS(
    priority_pool_bench.cpp
)

PEERDIR(
    ydb/library/actors/core
)

END()

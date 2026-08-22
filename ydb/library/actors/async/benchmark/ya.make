G_BENCHMARK(benchmark_actors_async)

SRCS(
    b_actor_async.cpp
)

PEERDIR(
    ydb/library/actors/async
    ydb/library/actors/core
)

END()

Y_BENCHMARK()

BENCHMARK_OPTS(--budget=10)

SRCS(
    main.cpp
)

END()

RECURSE(
    metrics
)

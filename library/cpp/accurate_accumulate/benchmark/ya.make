
Y_BENCHMARK()

BENCHMARK_OPTS(--budget=10)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/accurate_accumulate
)

END()

RECURSE(
    metrics
)

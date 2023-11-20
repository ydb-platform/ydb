Y_BENCHMARK()

BENCHMARK_OPTS(--budget=10)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
)

END()

RECURSE(
    metrics
)

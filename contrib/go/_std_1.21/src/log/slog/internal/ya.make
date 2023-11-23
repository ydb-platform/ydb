GO_LIBRARY()

SRCS(
    ignorepc.go
)

END()

RECURSE(
    benchmarks
    buffer
    slogtest
)

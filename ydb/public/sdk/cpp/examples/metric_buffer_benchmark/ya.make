PROGRAM(metric_buffer_benchmark)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/metrics
)

END()

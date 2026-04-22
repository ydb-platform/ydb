PROGRAM(otel_tracing_example)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/plugins/trace/otel
    ydb/public/sdk/cpp/plugins/metrics/otel
    contrib/libs/opentelemetry-cpp
    contrib/libs/opentelemetry-cpp/api
)

END()

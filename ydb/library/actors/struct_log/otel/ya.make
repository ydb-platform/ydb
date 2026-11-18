LIBRARY()

SRCS(
    otel_log_sink.cpp
)

PEERDIR(
    contrib/proto/opentelemetry
    ydb/library/actors/struct_log
)

END()

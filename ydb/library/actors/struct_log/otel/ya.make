LIBRARY()

SRCS(
    basic_otel_log_sink.cpp
    full_otel_log_sink.cpp
    simple_otel_log_sink.cpp
)

PEERDIR(
    contrib/proto/opentelemetry
    library/cpp/svnversion
    ydb/library/actors/core
    ydb/library/actors/struct_log
)

END()

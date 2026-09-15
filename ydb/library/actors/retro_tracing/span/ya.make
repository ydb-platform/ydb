LIBRARY()

SRCS(
    retro_span.cpp
    span_buffer.cpp
)

PEERDIR(
    contrib/proto/opentelemetry
    ydb/library/actors/wilson
)

END()

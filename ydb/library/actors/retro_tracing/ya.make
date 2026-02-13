LIBRARY()

SRCS(
    retro_collector.cpp
    retro_span.cpp
    span_buffer.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    collector/events.cpp
    collector/retro_collector.cpp
    collector/retro_span_deserialization.cpp
    span/retro_span.cpp
    span/span_buffer.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(
    ut
)

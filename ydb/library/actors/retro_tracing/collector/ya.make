LIBRARY()

SRCS(
    events.cpp
    retro_collector.cpp
    retro_span_deserialization.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    ydb/library/actors/core
    ydb/library/actors/retro_tracing/span
    ydb/library/actors/wilson
)

END()

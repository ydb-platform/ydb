LIBRARY()

SRCS(
    events.cpp
    retro_collector.cpp
    retro_span_deserialization.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(
    ut
)

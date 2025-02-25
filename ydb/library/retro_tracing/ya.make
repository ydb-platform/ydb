LIBRARY()

SRCS(
    retro_span.cpp
    retro_span_base.cpp
    retro_tracing.cpp
    span_circlebuf.cpp
    span_circlebuf_stats.cpp
)

PEERDIR(
    ydb/library/retro_tracing/protos
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(
    ut
)

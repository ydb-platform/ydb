LIBRARY()

SRCS(
    named_span.cpp
    retro_tracing.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/library/actors/retro_tracing
)

END()

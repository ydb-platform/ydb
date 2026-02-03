UNITTEST()

FORK_SUBTESTS()

SRCS(
    test_spans.cpp
    universal_span_ut.cpp
)

PEERDIR(
    ydb/library/actors/retro_tracing
)

END()

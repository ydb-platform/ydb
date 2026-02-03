LIBRARY()

SRCS(
    retro_span.cpp
)

PEERDIR(
    ydb/library/actors/wilson
)

END()

RECURSE_FOR_TESTS(
    ut
)

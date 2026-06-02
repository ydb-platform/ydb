LIBRARY()

SRCS(
    retro_span.cpp
    span_buffer.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

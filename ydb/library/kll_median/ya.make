LIBRARY()

PEERDIR(
    util
)

SRCS(
    sketch.h
    windowed_sketch.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
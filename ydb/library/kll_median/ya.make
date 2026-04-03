LIBRARY()

PEERDIR(
    util
)

SRCS(
    sketch.h
    dynamic_sketch.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
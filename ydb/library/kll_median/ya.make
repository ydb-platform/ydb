LIBRARY()

PEERDIR(
)

SRCS(
    sketch.h
    dynamic_sketch.h
    sketch.cpp
    dynamic_sketch.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

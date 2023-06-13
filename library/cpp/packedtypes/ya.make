LIBRARY()

PEERDIR(
    library/cpp/streams/zc_memory_input
)

SRCS(
    fixed_point.h
    longs.cpp
    packed.h
    packedfloat.cpp
    packedfloat.h
    zigzag.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    simple_leaky_bucket.cpp
    simple_leaky_bucket.h
)

PEERDIR(
)

END()

RECURSE_FOR_TESTS(
    ut
)

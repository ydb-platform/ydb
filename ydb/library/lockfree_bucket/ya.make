LIBRARY()

PEERDIR(
    library/cpp/time_provider
)

SRCS(
    light_bucket.cpp
    lockfree_bucket.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

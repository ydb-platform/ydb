LIBRARY()

SRCS(
    activation_queue.h
    mpmc_ring_queue.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

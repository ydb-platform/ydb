LIBRARY()

SRCS(
    activation_queue.h
    mpmc_ring_queue.h
)

END()

RECURSE_FOR_TESTS(
    ut
)

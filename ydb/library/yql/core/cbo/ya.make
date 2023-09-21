LIBRARY()

SRCS(cbo_optimizer.cpp)

PEERDIR(
    library/cpp/disjoint_sets
)

END()

RECURSE_FOR_TESTS(
    ut
)


LIBRARY()

SRCS(
    cbo_optimizer.cpp
    cbo_optimizer_new.cpp
)

PEERDIR(
    library/cpp/disjoint_sets
)

END()

RECURSE_FOR_TESTS(
    ut
)


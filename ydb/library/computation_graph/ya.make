LIBRARY()

SRCS(
    computation_graph.cpp
)

PEERDIR(
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)

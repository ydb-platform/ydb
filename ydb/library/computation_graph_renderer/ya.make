LIBRARY()

SRCS(
    computation_graph_renderer.cpp
)

PEERDIR(
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)

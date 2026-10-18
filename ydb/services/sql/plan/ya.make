LIBRARY()

SRCS(
    planner.cpp
)

PEERDIR(
    library/cpp/containers/absl
)

END()

RECURSE_FOR_TESTS(
    ut
)

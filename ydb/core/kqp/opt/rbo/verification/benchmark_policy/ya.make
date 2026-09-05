LIBRARY()

SRCS(
    coverage_policy.cpp
)

PEERDIR(
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)

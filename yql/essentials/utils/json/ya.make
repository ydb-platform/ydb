LIBRARY()

PEERDIR(
    library/cpp/json
    yql/essentials/utils/meta
)

SRCS(
    bidirectional.cpp
    expected.cpp
    from.cpp
    reflection.cpp
    to.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    benchmark
)
